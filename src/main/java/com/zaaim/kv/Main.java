package com.zaaim.kv;

import com.zaaim.kv.db.parser.DbParser;
import com.zaaim.kv.db.parser.KeyValuePair;
import com.zaaim.kv.parser.Parser;
import com.zaaim.kv.store.Cache;
import com.zaaim.kv.utils.Utils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class Main {
    public static void main(String[] args){
        System.out.println("Logs from your program will appear here!");
        Map<String, String> config = new HashMap<>();

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        config = new Utils().setConfig(args, config);
        int port = config.get("port") != null ? Integer.parseInt(config.get("port")) : 6379;

        try {
            String dbFilePath = config.get("dir") + "/" + config.get("dbfilename");
            System.out.println("DB File Path: " + dbFilePath);
            File f = new File(dbFilePath);

            if (f.exists()) {
                DataInputStream dataStream = new DataInputStream(new FileInputStream(dbFilePath));

                DbParser rdbParser = new DbParser(dataStream);
                ArrayList<KeyValuePair> data =  rdbParser.parse();
                for (KeyValuePair d : data) {
                    Cache.setCache(d);
                }
                dataStream.close();
            }

            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            boolean isSlave = Boolean.parseBoolean(config.get("isSlave"));
            if (isSlave) {
                Socket socket = new Socket(config.get("masterHost"), Integer.parseInt(config.get("masterPort")));
                new Thread(new SlaveConnection(socket, config)).start();
            }

            while (true) {
                clientSocket = serverSocket.accept();
                new Thread(new ClientConnHandler(clientSocket, config, isSlave)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}