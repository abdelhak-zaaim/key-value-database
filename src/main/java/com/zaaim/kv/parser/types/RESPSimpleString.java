package com.zaaim.kv.parser.types;

public record RESPSimpleString(String value) implements RESPObject {
    @Override
    public String toRedisString() {
        return "+" + this.value + "\r\n";
    }
}