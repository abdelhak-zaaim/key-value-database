package com.zaaim.kv.parser.types;


public record RESPBulkString(String value) implements RESPObject {
    @Override
    public String toRedisString() {
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
}