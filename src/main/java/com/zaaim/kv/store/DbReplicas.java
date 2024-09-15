package com.zaaim.kv.store;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;

public class DbReplicas {
    volatile private static HashSet<Replica> replicas = new HashSet<>();
    private static boolean prevWritesAvailable = false;

    public synchronized static void setReplica(Socket socket, BufferedWriter writer) throws IOException {
        replicas.add(new Replica(socket, writer));
    }

    public static void propagateCommand(List<String> commandArgs) {
        prevWritesAvailable = true;
        replicas.forEach(replica -> {
            try {
                replica.passCommand(commandArgs);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Replica findReplica(Socket socket) {
        return replicas.stream()
                .filter(replica -> replica.getSocket().getPort() == socket.getPort())
                .findFirst()
                .orElse(null);
    }

    public static int getAllSyncedReplicas(int numOfReplicasNeeded, long timeToWait) {
        Instant start = Instant.now();
        int result = 0;

        if (!prevWritesAvailable) {
            return replicas.size();
        }

        replicas.forEach(replica -> {
            try {
                replica.sendAck();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        while (Duration.between(start, Instant.now()).toMillis() < timeToWait) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}

            result = (int) replicas.stream().filter(Replica::isSyncedWithMaster).count();

            if (result >= numOfReplicasNeeded) {
                return result;
            }
        }

        return result;
    }

}