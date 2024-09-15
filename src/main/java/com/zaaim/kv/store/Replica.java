package com.zaaim.kv.store;

import com.zaaim.kv.parser.types.RESPArray;
import com.zaaim.kv.parser.types.RESPBulkString;
import com.zaaim.kv.parser.types.RESPObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;

public class Replica {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private int currentOffset = 0;
    private int desiredOffset = 0;
    private final static int REPLCONFGETACKSIZE = 37;

    public Replica(Socket socket, BufferedWriter writer) throws IOException {
        this.socket = socket;
        this.writer = writer;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    public Socket getSocket() {
        return socket;
    }

    public void setCurrentOffset(int currentOffset) {
        this.currentOffset = currentOffset;
    }

    public void passCommand(List<String> commandArgs) throws IOException {
        String command = constructCommand(commandArgs);
        desiredOffset += command.length();
        writer.write(command);
        writer.flush();
    }

    public void sendAck() throws IOException {
        List<RESPObject> commandArr = List.of(new RESPBulkString("REPLCONF"), new RESPBulkString("GETACK"), new RESPBulkString("*"));
        String command = new RESPArray(commandArr).toRedisString();
        desiredOffset += command.length();
        writer.write(command);
        writer.flush();
    }

    public boolean isSyncedWithMaster() {
        return desiredOffset - REPLCONFGETACKSIZE <= currentOffset;
    }

    private String constructCommand(List<String> commandArgs) {
        List<RESPObject> command = commandArgs.stream().map(c -> (RESPObject) new RESPBulkString(c)).toList();
        return new RESPArray(command).toRedisString();
    }
}