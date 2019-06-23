package com.redislabs.redistimeseries;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Command implements ProtocolCommand{

    CREATE("TS.CREATE"),
    RANGE("TS.RANGE"),
    MRANGE("TS.MRANGE"),
    CREATE_RULE("TS.CREATERULE"),
    DELETE_RULE("TS.DELETERULE"),
    ADD("TS.ADD"),
    MADD("TS.MADD"),
    INCRBY("TS.INCRBY"),
    DECRBY("TS.DECRBY"),
    INFO("TS.INFO");
    
    private final byte[] raw;

    Command(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}