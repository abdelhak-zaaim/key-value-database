package com.zaaim.kv.parser.types;


public record RESPInteger(int num) implements RESPObject {
    @Override
    public String toRedisString() {
        return ":" + num + "\r\n";
    }
}