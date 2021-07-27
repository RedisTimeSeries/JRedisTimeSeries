package com.redislabs.redistimeseries;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Command implements ProtocolCommand {
  CREATE("TS.CREATE"),
  RANGE("TS.RANGE"),
  REVRANGE("TS.REVRANGE"),
  MRANGE("TS.MRANGE"),
  MREVRANGE("TS.MREVRANGE"),
  CREATE_RULE("TS.CREATERULE"),
  DELETE_RULE("TS.DELETERULE"),
  ADD("TS.ADD"),
  MADD("TS.MADD"),
  DEL("TS.DEL"),
  INCRBY("TS.INCRBY"),
  DECRBY("TS.DECRBY"),
  INFO("TS.INFO"),
  GET("TS.GET"),
  MGET("TS.MGET"),
  ALTER("TS.ALTER"),
  QUERYINDEX("TS.QUERYINDEX");

  private final byte[] raw;

  Command(String alt) {
    raw = SafeEncoder.encode(alt);
  }

  @Override
  public byte[] getRaw() {
    return raw;
  }
}
