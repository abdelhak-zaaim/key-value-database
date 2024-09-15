package com.zaaim.kv.utils;

import java.util.Map;

public class Utils {

    public Map<String, String> setConfig(String[] args, Map<String, String> config) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                    if (i + 1 < args.length) {
                        config.put("dir", args[i + 1]);
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        config.put("dbfilename", args[i + 1]);
                    }
                    break;
                case "--port":
                case "-p":
                    if (i + 1 < args.length) {
                        config.put("port", args[i + 1]);
                    }
                    break;
                case "--replicaof":
                    if (i + 1 < args.length) {
                        String replicaInfo = args[i + 1];
                        String[] masterInfo = replicaInfo.split(" ");
                        if (masterInfo.length > 1) {
                            String masterHost = masterInfo[0];
                            String masterPort = masterInfo[1];
                            config.put("masterHost", masterHost);
                            config.put("masterPort", masterPort);
                            config.put("isSlave", "true");
                        }
                    }
            }
        }
        return config;
    }
}