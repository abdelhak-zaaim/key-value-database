package com.zaaim.kv.db.parser;


import java.sql.Timestamp;

public class KeyValuePair {
    String key;
    ValueType type;
    Object value;
    Timestamp expiryTime;

    public KeyValuePair() {}

//    public KeyValuePair(String key, Object value, ValueType type, String id) {
//        this.key = key;
//        this.value = value;
//        this.type = type;
//    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public ValueType getType() {
        return type;
    }

    public void setType(ValueType type) {
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Timestamp getExpiryTime() {
        return expiryTime;
    }

    public void setExpiryTime(Timestamp expiryTime) {
        this.expiryTime = expiryTime;
    }

    public boolean isNumerical() {
        try {
            Integer.parseInt(value.toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}