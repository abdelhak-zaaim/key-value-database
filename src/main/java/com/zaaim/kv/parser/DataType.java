package com.zaaim.kv.parser;

public enum DataType {
    SIMPLE_STRING('+'),
    SIMPLE_ERROR('-'),
    INTEGER(':'),
    BULK_STRING('$'),
    ARRAY('*'),
    MAP('%'),
    SET('~'),
    NULL('_'),
    BOOLEAN('#'),
    DOUBLE(','),
    BIG_NUMBER('('),
    BULK_ERROR('!'),
    VERBATIM_STRING('='),
    PUSHES('>');

    private final char prefix;

    DataType(char prefix) {
        this.prefix = prefix;
    }

    public static DataType getTypeFromPrefix(char prefix) {
        for (DataType type : values()) {
            if (type.prefix == prefix) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown RESP encoding: " + prefix);
    }
}
