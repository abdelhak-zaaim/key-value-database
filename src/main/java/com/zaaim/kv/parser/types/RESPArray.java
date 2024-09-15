package com.zaaim.kv.parser.types;


import java.util.List;

public record RESPArray(List<RESPObject> values) implements RESPObject {
    @Override
    public String toRedisString() {
        StringBuilder sb = new StringBuilder("*" + values.size() + "\r\n");
        for (RESPObject value : values) {
            sb.append(value.toRedisString());
        }
        return sb.toString();
    }
}