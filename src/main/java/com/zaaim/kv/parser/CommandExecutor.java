package com.zaaim.kv.parser;

import com.zaaim.kv.db.parser.KeyValuePair;
import com.zaaim.kv.db.parser.ValueType;
import com.zaaim.kv.parser.types.*;
import com.zaaim.kv.store.Cache;
import com.zaaim.kv.store.DbReplicas;
import com.zaaim.kv.store.Replica;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class CommandExecutor {
    Socket socket;
    BufferedWriter writer;
    Map<String, String> config;

    public CommandExecutor(Socket socket, BufferedWriter writer, Map<String, String> config) {
        this.socket = socket;
        this.writer = writer;
        this.config = config;
    }

    public String execute(RESPArray command) throws IOException {
        List<RESPObject> items = command.values();
        if (items.isEmpty()) {
            return "-Err Empty command\r\n";
        }
        List<String> commandArgs = extractCommandsArgsToString(items);

        String redisCommand = commandArgs.getFirst();
        String commandName = redisCommand.toUpperCase();

        return switch (Commands.valueOf(commandName)) {
            case PING -> new RESPSimpleString("PONG").toRedisString();
            case ECHO -> items.get(1).toRedisString();
            case SET -> executeSet(commandArgs);
            case GET -> executeGet(commandArgs);
            case CONFIG -> executeConfig(commandArgs);
            case KEYS -> executeKeys(commandArgs);
            case INFO -> executeInfo(commandArgs);
            case REPLCONF -> executeReplConf(commandArgs);
            case PSYNC -> executePsync(commandArgs);
            case WAIT -> executeWait(commandArgs);
            case TYPE -> executeType(commandArgs);
            case XADD -> executeXadd(commandArgs);
            case XRANGE -> executeXrange(commandArgs);
            case XREAD -> executeXread(commandArgs);
            case INCR -> executeIncr(commandArgs);
            default -> "-ERR Unknown command\r\n";
        };
    }

    private String executeSet(List<String> command) {
        if (command.size() < 3) {
            return "-Err incorrect number of arguments for 'set' command\r\n";
        }
        boolean hasExtraArgs = command.size() > 3;

        String key = command.get(1);
        String value = command.get(2);

        KeyValuePair entry = new KeyValuePair();
        entry.setValue(value);
        entry.setKey(key);
        entry.setType(entry.isNumerical() ? ValueType.INTEGER : ValueType.STRING);

        if (hasExtraArgs) {
            String expiry = command.get(4);
            long expiryValue = new Date().getTime() + Long.parseLong(expiry);
            entry.setExpiryTime(new Timestamp(expiryValue));
        }
        Cache.setCache(entry);
        DbReplicas.propagateCommand(command);
        return new RESPSimpleString("OK").toRedisString();
    }

    private String executeGet(List<String> command) {
        if (command.size() < 2) {
            return "-Err incorrect number of arguments for 'get' command\r\n";
        }

        String key = command.get(1);
        List<KeyValuePair> listEntry = Cache.getCache().stream().filter(item -> item.getKey().equals(key)).toList();
        if (listEntry!=null && listEntry.size() > 1) {
            return "-Err multiple entries found for key (" + key + ")\r\n";
        }
        KeyValuePair entry =  listEntry.getFirst();
        boolean hasExpired = false;

        if (entry.getExpiryTime() != null) {
            hasExpired = entry.getExpiryTime().getTime() < new Date().getTime();
        }
        return (entry == null || hasExpired) ? "$-1\r\n" : "$" + entry.getValue().toString().length() + "\r\n" + entry.getValue() + "\r\n";
    }

    private String executeConfig(List<String> command) {
        if (command.size() < 3) {
            return "-Err invalid number of parameters for 'config get' command";
        }

        String key = command.get(2);

        if (key.equals("dir")) {
            String dir = config.get("dir");
            List<RESPObject> response = List.of(new RESPBulkString("dir"), new RESPBulkString(dir));
            return new RESPArray(response).toRedisString();
        }

        if (key.equals("dbfilename")) {
            String dbFileName = config.get("dbfilename");
            List<RESPObject> response = List.of(new RESPBulkString("dbfilename"), new RESPBulkString(dbFileName));
            return new RESPArray(response).toRedisString();
        }

        return new RESPArray(List.of(new RESPBulkString("-1"))).toRedisString();
    }

    private String executeKeys(List<String> command) {
        if (command.size() < 2) {
            return "-Err Invalid number of parameters for 'keys' command";
        }
        String keyName = command.get(1);
        List<String> keys = Cache.getCache().stream().map(KeyValuePair::getKey).toList();

        if (keyName.equals("*")) {
            List<RESPObject> response = new ArrayList<>();
            for (String k : keys) {
                response.add(new RESPBulkString(k));
            }
            return new RESPArray(response).toRedisString();
        } else {
            String key = keys.stream().filter(i -> i.equals(keyName)).findFirst().orElse(null);
            return new RESPBulkString(key).toRedisString();
        }
    }

    private String executeInfo(List<String> commands) {
        String infoArgument = "";
        if (commands.size() > 1) {
            infoArgument = commands.get(1).toUpperCase();
        }

        if (infoArgument.equals("REPLICATION")) {
            boolean isSlave = config.containsKey("masterHost");
            String type = isSlave ? "role:slave" : "role:master";
            String masterReplid = "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            String masterReplOffset = "master_repl_offset:0";
            String res = masterReplOffset + masterReplid + type;
            return new RESPBulkString(res).toRedisString();
        };
        return null;
    }

    private String executePsync(List<String> commands) {
        if (commands.size() < 3) {
            return "-Err Invalid number of arguments for 'psync' command";
        }
        String replID = commands.get(1);
        String offset = commands.get(2);

        return "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0 \r\n";
    }

    private String executeReplConf(List<String> command) {
        if (command.size() < 3) {
            return "-Err Invalid number of commands for 'replconf command'";
        }

        String commandArg = command.get(1);
        String cacheOffset = Integer.toString(Cache.getOffset());

        return switch (commandArg) {
            case "listening-port", "capa" -> "+OK\r\n";
            case "GETACK" -> "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + cacheOffset.length() + "\r\n" + cacheOffset + "\r\n";
            case "ACK" -> {
                int offset = Integer.parseInt(command.get(2));
                Replica replica = DbReplicas.findReplica(socket);
                replica.setCurrentOffset(offset);
                yield null;
            }
            default -> null;
        };
    }

    private String executeWait(List<String> command) {
        if (command.size() < 3) {
            return "-Err Invalid number of arguments for 'WAIT' command";
        }

        int numOfReplicasNeeded = Integer.parseInt(command.get(1));
        int timeTowait = Integer.parseInt(command.get(2));

        int numOfReplicasToAck = DbReplicas.getAllSyncedReplicas(numOfReplicasNeeded, timeTowait);
        return new RESPInteger(numOfReplicasToAck).toRedisString();
    }

    private String executeType(List<String> command) {
        String key = command.get(1);
        KeyValuePair entry = Cache.getCache().stream().filter(i -> i.getKey().equals(key)).findFirst().orElse(null);
        if (entry == null) {
            return new RESPSimpleString("none").toRedisString();
        }

        String keyType = entry.getType().getTypeName();
        return new RESPSimpleString(keyType).toRedisString();
    }

    private String executeXadd(List<String> commandArgs) {
        if (commandArgs.size() < 5) {
            return "-Err Invalid number of arguments for 'xadd' command";
        }

        String streamKey = commandArgs.get(1);
        String streamEntryId = commandArgs.get(2);
        List<String> streamEntriesList = commandArgs.subList(3, commandArgs.size());
        boolean isFullGeneratedId = streamEntryId.equals("*");

        KeyValuePair stream = Cache.getCache()
                .stream()
                .filter(it -> it.getKey().equals(streamKey))
                .findFirst()
                .orElse(null);

        RESPStream streamInstance;

        if (stream != null) {
            streamInstance = (RESPStream) stream.getValue();
        } else {
            streamInstance = new RESPStream();
        }

        RESPStream.RespStreamEntry streamEntry = streamInstance.new RespStreamEntry();
        String isStreamIdValidErrorMsg = streamInstance.isStreamEntryValid(streamEntryId);
        if (!isStreamIdValidErrorMsg.isEmpty()) {
            return isStreamIdValidErrorMsg;
        }

        streamEntryId = streamInstance.createNextId(streamEntryId);
        streamEntry.setId(streamEntryId);
        for (int i = 0; i < streamEntriesList.size(); i += 2) {
            streamEntry.addStreamEntry(Map.of(streamEntriesList.get(i), streamEntriesList.get(i+1)));
        }
        streamInstance.addStreamEntry(streamEntry);

        KeyValuePair entry = new KeyValuePair();
        entry.setKey(streamKey);
        entry.setType(ValueType.STREAM);
        entry.setValue(streamInstance);
        Cache.setCache(entry);

        return isFullGeneratedId ? new RESPBulkString(streamEntryId).toRedisString() : new RESPSimpleString(streamEntryId).toRedisString();
    }

    private String executeXrange(List<String> command) {
        if (command.size() < 4) {
            return "-Err Invalid number of arguments for 'XRANGE' command";
        }

        String streamKey = command.get(1);
        String streamEntryStartId = command.get(2);
        String streamEntryEndId = command.get(3);

        KeyValuePair entry = Cache.getCache()
                .stream()
                .filter(e -> e.getKey().equals(streamKey))
                .findFirst()
                .orElse(null);

        if (entry == null) {
            return "-Err no item for key (" + streamKey + ") found";
        }

        RESPStream value = (RESPStream) entry.getValue();
        List<RESPStream.RespStreamEntry> streamEntries = value.getStreamEntriesWithinRange(streamEntryStartId, streamEntryEndId);
        List<RESPObject> streamEntriesAsRedisArray = streamEntries.stream().map(e -> e.convertStreamToRedisArray()).toList();
        return new RESPArray(streamEntriesAsRedisArray).toRedisString();
    }

    private String executeXread(List<String> command) {
        if (command.size() < 4) {
            return "-Err Invalid number of arguments for 'XREAD' command";
        }

        // get stream keys and stream entry Ids
        // number of keys is equal to number of stream ids
        List<String> streamsArgs;
        long timeToBlock = -1;
        boolean isBlockingRead = command.get(1).equalsIgnoreCase("block");
        boolean isInfiniteLoop = false;

        if (isBlockingRead) {
            streamsArgs = command.subList(4, command.size());
            timeToBlock = Long.parseLong(command.get(2));
            isInfiniteLoop = timeToBlock == 0;
        } else {
            streamsArgs = command.subList(2, command.size());
        }

        if (streamsArgs.size() % 2 != 0) {
            return "-Err Invalid number of arguments for 'XREAD' command";
        }

        int numOfStreams = streamsArgs.size() / 2;

        ArrayList<KeyValuePair> streamsFromCache = new ArrayList<>();
        for (int i = 0; i < numOfStreams; i++) {
            final int index = i;
            KeyValuePair entry = Cache.getCache()
                    .stream()
                    .filter(e -> e.getKey().equals(streamsArgs.get(index)))
                    .findFirst()
                    .orElse(null);
            streamsFromCache.add(entry);
        }

        // blocking read
        Instant start = Instant.now();
        boolean newDataAvailable = false;
        String previousStreamEntryId = "";

        while (timeToBlock == 0 || Duration.between(start, Instant.now()).toMillis() < timeToBlock) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }

            for (int i = 0; i < streamsFromCache.size(); i++) {
                KeyValuePair entry = streamsFromCache.get(i);
                RESPStream stream = (RESPStream) entry.getValue();

                // position of stream entry id is (numOfStreams) ahead
                String streamEntryId = streamsArgs.get(i + numOfStreams);
                // get last entry for "$" Id
                if (streamEntryId.equals("$")) {
                    streamEntryId = stream.getPreviousStreamEntryId();
                }

                String nextStreamEntryId = stream.getNextStreamId(streamEntryId);
                List<RESPStream.RespStreamEntry> streamEntries = stream.getStreamEntriesWithinRange(nextStreamEntryId, "+");
                if (!streamEntries.isEmpty()) {
                    newDataAvailable = true;
                }
            }
            if (isInfiniteLoop && newDataAvailable) {
                break;
            }
        }

        if (!newDataAvailable && isBlockingRead) {
            return "$-1\r\n";
        }

        ArrayList<RESPObject> streamsWithKeysRedisArray = new ArrayList<>();
        for (int i = 0; i < streamsFromCache.size(); i++) {
            KeyValuePair entry = streamsFromCache.get(i);
            RESPStream stream = (RESPStream) entry.getValue();

            // position of stream entry id is (numOfStreams) ahead
            String streamEntryId = streamsArgs.get(i + numOfStreams);
            if (streamEntryId.equals("$")) {
                streamEntryId = stream.getPreviousStreamEntryId();
            }

            String nextStreamEntryId = stream.getNextStreamId(streamEntryId);
            List<RESPStream.RespStreamEntry> streamEntries = stream.getStreamEntriesWithinRange(nextStreamEntryId, "+");
            List<RESPObject> streamEntriesAsRedisArrayList = streamEntries.stream().map(e -> e.convertStreamToRedisArray()).toList();
            RESPObject streamEntriesCombined = new RESPArray(streamEntriesAsRedisArrayList);
            RESPObject streamWithKeyAndEntriesAsRedisArray = new RESPArray(List.of(new RESPBulkString(entry.getKey()), streamEntriesCombined));
            streamsWithKeysRedisArray.add(streamWithKeyAndEntriesAsRedisArray);
        }

        return new RESPArray(streamsWithKeysRedisArray).toRedisString();
    }

    private String executeIncr(List<String> command) {
        if (command.size() < 2) {
            return "-Err Invalid number of argument for 'INCR' command";
        }

        String key = command.get(1);

        KeyValuePair entry = Cache.getCache()
                .stream()
                .filter(e -> e.getKey().equals(key))
                .findFirst()
                .orElse(null);

        if (entry == null) {
            KeyValuePair newEntry = new KeyValuePair();
            newEntry.setKey(key);
            newEntry.setValue(1);
            newEntry.setType(ValueType.INTEGER);
            Cache.setCache(newEntry);
            return new RESPInteger(1).toRedisString();
        } else if (entry.getType().equals(ValueType.INTEGER)) {
            int nextValue = Integer.parseInt(entry.getValue().toString()) + 1;
            entry.setValue(nextValue);
            return new RESPInteger(nextValue).toRedisString();
        } else {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }

    private List<String> extractCommandsArgsToString(List<RESPObject> command) {
        return command.stream().map(i -> ((RESPBulkString) i).value()).toList();
    }

}