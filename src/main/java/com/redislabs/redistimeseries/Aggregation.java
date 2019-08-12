package com.redislabs.redistimeseries;

import redis.clients.jedis.util.SafeEncoder;

public enum Aggregation {
  AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, 
  STD_P("STD.P"), STD_S("STD.S"), STD_V("STD.V");  
  
  private final byte[] raw;
  
  Aggregation(String alt) {
    raw = SafeEncoder.encode(alt);
  }
  
  Aggregation() {
    raw = SafeEncoder.encode(this.name());
  }

  public byte[] getRaw() {
    return raw;
  }
}

