package com.zaaim.kv.db.parser;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DbParser {
    private static final Charset ASCII = StandardCharsets.US_ASCII;

    private static final int EOF = 0xFF;
    private static final int SELECTDB = 0xFE;
    private static final int EXPIRETIME = 0xFD;
    private static final int EXPIRETIMEMS = 0xFC;
    private static final int RESIZEDB = 0xFB;
    private static final int AUX = 0xFA;

    private static final int RDB_6BIT_ENC = 0;
    private static final int RDB_14BIT_ENC = 1;
    private static final int RDB_32BIT_ENC = 0x80;
    private static final int RDB_64BIT_ENC = 0x81;
    private static final int RDB_ENVVAL = 3;

    private static final int RDB_STR_8BIT_ENC = 0;
    private static final int RDB_STR_16BIT_ENC = 1;
    private static final int RDB_STR_32BIT_ENC = 2;
    private static final int RDB_STR_LZF_COMP = 3;

    private static final int RDB_ENC_BIT8 = 0;
    private static final int RDB_ENC_BIT16 = 1;
    private static final int RDB_ENC_BIT32 = 2;

    private DataInputStream inputStream;
    private ArrayList<KeyValuePair> keyValuePairs = new ArrayList<>();
    private KeyValuePair nextEntry;

    public DbParser(DataInputStream inputStream) {
        this.inputStream = inputStream;
    }

    private int readByte() throws IOException {
        int z = inputStream.readByte();
        return z & 0xff;
    }

    private int readSignedByte() throws IOException {
        return inputStream.readByte();
    }

    private int readShort() throws IOException {
        return inputStream.readShort();
    }

    private int readInt() throws IOException {
        return inputStream.readInt();
    }

    private byte[] readBytes(int numOfBytes) throws IOException {
        byte[] b = new byte[numOfBytes];
        inputStream.readFully(b);
        return b;
    }

    private void parseHeader() throws IOException {
        byte[] header = new byte[9];
        inputStream.readFully(header);
        String headerStr = new String(header, ASCII);
        String redis = headerStr.substring(0,5);
        int version = Integer.parseInt(headerStr.substring(5));

        if (!redis.equals("REDIS")) {
            throw new IOException("Invalid RDB file header");
        }

        if (version < 1) {
            throw new IOException("Unknown version");
        }
    }

    public ArrayList<KeyValuePair> parse() throws IOException {
        parseHeader();
        nextEntry = new KeyValuePair();

        while (inputStream.available() > 0) {
            int type = readByte();
            if (type == EOF) {
                break;
            }

            switch (type) {
                case SELECTDB:
                    readSelectDB();
                    break;
                case RESIZEDB:
                    readResizeDb();
                    break;
                case AUX:
                    readAux();
                    break;
                case EXPIRETIME:
                    readExpiryTime();
                    continue;
                case EXPIRETIMEMS:
                    readExpiryTimeMilli();
                    continue;
                default:
                    readEntry(type);
                    keyValuePairs.add(nextEntry);
                    nextEntry = new KeyValuePair();
                    break;
            }
        }
        return keyValuePairs;
    }

    private int readLength() throws IOException {
        int firstByte = readByte();
        int type = (firstByte >> 6) & 0x03;

        if (type == RDB_ENVVAL) {
            return firstByte & 0x3f;
        } else if (type == RDB_6BIT_ENC) {
            return firstByte & 0x3f;
        } else if (type == RDB_14BIT_ENC) {
            return ((firstByte & 0x3f) << 8) | readByte();
        } else if (firstByte == RDB_32BIT_ENC) {
            return inputStream.readInt();
        } else if (firstByte == RDB_64BIT_ENC) {
            byte[] b = readBytes(8);
            return ByteBuffer.wrap(b).getInt();
        } else {
            throw new IOException("Unknown encoding type");
        }
    }

    private void readSelectDB() throws IOException {
        int id = readLength();
        String res = "SELECT DB (" + id + ")";
        System.out.println(res);
    }

    private void readResizeDb() throws IOException {
        int databaseHashSize = readLength();
        int expiryHashSize = readLength();

        String res = "Resize DB {" + databaseHashSize + ", " + expiryHashSize + "}";
        System.out.println(res);
    }

    private void readAux() throws IOException {
        String key = parseStringEncdoded();
        String value = parseStringEncdoded();

        String res = "Auxiliary: { key: " + key + ", value: " + value + " }";
        System.out.println(res);
    }

    private String parseStringEncdoded() throws IOException {
        int firstByte = readByte();
        int type = (firstByte >> 6) & 0x03;
        int len;
        byte[] data;

        switch (type) {
            case RDB_6BIT_ENC:
                len = firstByte & 0x3f;
                data = new byte[len];
                inputStream.readFully(data);
                return new String(data, ASCII);
            case RDB_14BIT_ENC:
                len = ((firstByte & 0x3f) << 8) | readByte();
                data = readBytes(len);
                return new String(data, ASCII);
            case RDB_32BIT_ENC:
                len = readInt();
                data = readBytes(len);
                return new String(data, ASCII);
            case RDB_ENVVAL:
                return parseSpecialStringEncoded(firstByte & 0x3f);
            default:
                return null;
        }
    }

    private String parseDoubleScoreString() throws IOException {
        int len = readByte();

        return switch (len) {
            case 0xff -> String.valueOf(Double.NEGATIVE_INFINITY);
            case 0xfe -> String.valueOf(Double.POSITIVE_INFINITY);
            case 0xfd -> String.valueOf(Double.NaN);
            default -> {
                byte[] data = readBytes(len);
                yield new String(data, ASCII);
            }
        };
    }

    private String parseSpecialStringEncoded(int type) throws IOException {
        int value;

        return switch (type) {
            case RDB_STR_8BIT_ENC -> {
                value = readSignedByte();
                yield String.valueOf(value);
            }
            case RDB_STR_16BIT_ENC -> {
                value = readShort();
                yield String.valueOf(value);
            }
            case RDB_STR_32BIT_ENC -> {
                value = readInt();
                yield String.valueOf(value);
            }
            case RDB_STR_LZF_COMP -> parseLzfCompressedStr();
            default -> throw new IllegalArgumentException("Unknown string special encoding " + type);
        };
    }

    private String parseLzfCompressedStr() throws IOException {
        int clen = readLength();
        int ulen = readLength();
        byte[] data = readBytes(clen);
        byte[] dest = new byte[ulen];
        LZF.expand(data, 0, dest, 0, ulen);
        return new String(dest, ASCII);
    }

    private void readString() throws IOException {
        String key = parseStringEncdoded();
        String value = parseStringEncdoded();
        System.out.println("{ key: " + key + ", value: " + value + " }");

        nextEntry.setKey(key);
        nextEntry.setValue(value);
        nextEntry.setType(ValueType.STRING);
    }

    private void readList() throws IOException {
        String key = parseStringEncdoded();
        int size = readLength();

        List<String> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(parseStringEncdoded());
        }

        nextEntry.setKey(key);
        nextEntry.setValue(list);
        nextEntry.setType(ValueType.LIST);
    }

    private void readSet() throws IOException {
        String key = parseStringEncdoded();
        int size = readLength();

        List<String> set = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            set.add(parseStringEncdoded());
        }

        nextEntry.setKey(key);
        nextEntry.setValue(set);
        nextEntry.setType(ValueType.SET);
    }

    private void readSortedSet() throws IOException {
        String key = parseStringEncdoded();
        int size = readLength();

        List<String> valueScorePairs = new ArrayList<>(size * 2);
        for (int i = 0; i < size; i++) {
            valueScorePairs.add(parseStringEncdoded());
            valueScorePairs.add(parseDoubleScoreString());
        }

        nextEntry.setKey(key);
        nextEntry.setValue(valueScorePairs);
        nextEntry.setType(ValueType.SORTED_SET);
    }

    private void readHash() throws IOException {
        String key = parseStringEncdoded();
        System.out.println("HASH key => " + key);
        int size = readLength();
        System.out.println("Hash size => " + size);

        HashMap<String, String> hash = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String mapKey = parseStringEncdoded();
            String mapValue = parseStringEncdoded();
            System.out.println("{ key: " + mapKey + ", value: " + mapValue + " }");
            hash.put(mapKey, mapValue);
        }

        nextEntry.setKey(key);
        nextEntry.setValue(hash);
        nextEntry.setType(ValueType.HASH);
    }

    private void readEntry(int type) throws IOException {
        switch (type) {
            case 0:
                readString();
                break;
            case 1:
                readList();
                break;
            case 2:
                readSet();
                break;
            case 3:
                readSortedSet();
                break;
            case 4:
                readHash();
                break;
            default:
                throw new UnsupportedOperationException("Unknown value type: " + type);
        }
    }

    private void readExpiryTime() throws IOException {
        byte[] b = readBytes(4);
        long expiry = ((long) b[3] & 0xff) << 24 | ((long) b[2] & 0xff) << 16 | ((long) b[1] & 0xff) << 8 | ((long) b[0] & 0xff);
        Timestamp expT = new Timestamp(expiry * 1000);
        System.out.println("Expiry ==> Second " + expiry + " =>> " + expT);
        nextEntry.setExpiryTime(expT);
    }

    private void readExpiryTimeMilli() throws IOException {
        byte[] b = readBytes(8);
        long expiry = ((long) b[7] & 0xff) << 56 // reverse as it's in little endian format
                | ((long) b[6] & 0xff) << 48
                | ((long) b[5] & 0xff) << 40
                | ((long) b[4] & 0xff) << 32
                | ((long) b[3] & 0xff) << 24
                | ((long) b[2] & 0xff) << 16
                | ((long) b[1] & 0xff) << 8
                | ((long) b[0] & 0xff);
        Timestamp expT = new Timestamp(expiry);
        System.out.println("Expiry Milli => " + expiry + " => " + expT);
        nextEntry.setExpiryTime(expT);
    }

}