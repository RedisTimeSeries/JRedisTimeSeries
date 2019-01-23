package com.redislabs.redistimeseries;

import redis.clients.jedis.util.SafeEncoder;

public enum Aggregation {
  AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST;
  
  private final byte[] raw;
  
  Aggregation() {
    raw = SafeEncoder.encode(this.name());
  }

  public byte[] getRaw() {
    return raw;
  }
}

