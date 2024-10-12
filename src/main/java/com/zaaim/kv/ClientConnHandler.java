package com.zaaim.kv;

import java.io.*;
import java.net.Socket;
import java.util.Base64;
import java.util.Map;
import com.zaaim.kv.parser.*;
import com.zaaim.kv.parser.types.RESPArray;
import com.zaaim.kv.parser.types.RESPObject;
import com.zaaim.kv.store.DbReplicas;

public class ClientConnHandler implements Runnable {
    private static String emptyRDBFileContent = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    private final Socket socket;
    Map<String, String> config;
    boolean readAfterHandShake = false;

    public ClientConnHandler(Socket socket, Map<String, String> config, boolean readAfterHandShake) {
        this.socket = socket;
        this.config = config;
        this.readAfterHandShake = readAfterHandShake;

        if (readAfterHandShake) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            Parser parser = new Parser(reader);

            while (true) {
                RESPObject command = parser.parse();
                OutputStream outputStream = socket.getOutputStream();

                if (command instanceof RESPArray) {
                    System.out.println("Command: " + command);
                    String argument = new CommandExecutor(socket, writer, config).execute((RESPArray) command);
                    if (argument == null) {
                        continue;
                    }
                    writer.write(argument);

                    if (argument.contains("FULLRESYNC")) {
                        sendEmptyRDBFile(writer, outputStream);
                        DbReplicas.setReplica(socket, writer);
                    }
                    writer.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
//            try {
//                if (socket != null) {
//                    socket.close();
//                }
//            } catch (IOException e) {
//                System.out.println("IOException: " + e.getMessage());
//            }
        }
    }

    private void sendEmptyRDBFile(BufferedWriter writer, OutputStream os) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(emptyRDBFileContent);
        writer.write(("$" + bytes.length + "\r\n"));
        writer.flush();
        os.write(bytes);
    }
}