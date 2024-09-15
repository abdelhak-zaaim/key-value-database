package com.zaaim.kv;

import com.zaaim.kv.parser.CommandExecutor;
import com.zaaim.kv.parser.Parser;
import com.zaaim.kv.parser.types.RESPArray;
import com.zaaim.kv.parser.types.RESPObject;
import com.zaaim.kv.store.Cache;

import java.io.*;
import java.net.Socket;
import java.util.Map;

public class SlaveConnection implements Runnable {
    Socket slave;
    Map<String, String> config;

    public SlaveConnection(Socket socket, Map<String, String> config) {
        this.slave = socket;
        this.config = config;
    }

    public void run() {
        try {
            OutputStream outputStream = slave.getOutputStream();
            InputStream inputStream = slave.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
            performHandshake(writer, reader);

            Parser parser = new Parser(reader);
            while (true) {
                RESPObject command = parser.parse();
                if (command == null) {
                    continue;
                }
                Cache.setCurrOffset();
                Cache.setOffset(command.toRedisString().length());
                String response = new CommandExecutor(slave, writer, config).execute((RESPArray) command);
                if (response.contains("ACK")) {
                    writer.write(response);
                    writer.flush();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void performHandshake(BufferedWriter writer, BufferedReader reader) throws IOException {
        writer.write("*1\r\n$4\r\nPING\r\n");
        writer.flush();
        reader.readLine();
        writer.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n");
        writer.flush();
        reader.readLine();
        writer.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        writer.flush();
        reader.readLine();
        writer.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        writer.flush();
        reader.readLine(); // read FULLRESYNC

        readEmptyRDBFile(reader);
    }

    private void readEmptyRDBFile(BufferedReader reader) throws IOException {
        if (reader.read() == -1) {
            return;
        }
        int length = Integer.parseInt(reader.readLine());
        char[] data = new char[length-1];
        reader.read(data);
    }
}