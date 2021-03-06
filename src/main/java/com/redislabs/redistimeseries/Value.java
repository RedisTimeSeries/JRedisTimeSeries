package com.redislabs.redistimeseries;

import java.util.List;
import redis.clients.jedis.util.SafeEncoder;

public class Value {

  private final long time;
  private final double val;

  public Value(long time, double value) {
    this.time = time;
    this.val = value;
  }

  public long getTime() {
    return time;
  }

  public double getValue() {
    return val;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Value)) return false;

    Value other = (Value) o;
    return other.time == this.time && other.val == this.val;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(time) ^ Double.hashCode(val);
  }

  @Override
  public String toString() {
    return "(" + this.time + ":" + this.val + ")";
  }

  protected static Value parseValue(List<Object> tuple) {
    return new Value(
        (Long) tuple.get(0), Double.parseDouble(SafeEncoder.encode((byte[]) tuple.get(1))));
  }
}
