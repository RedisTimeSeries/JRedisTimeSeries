package com.redislabs.redistimeseries;

import static redis.clients.jedis.BuilderFactory.DOUBLE;
import static redis.clients.jedis.BuilderFactory.LONG;

import java.util.List;
import java.util.stream.Collectors;
import redis.clients.jedis.Builder;
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

  static final Builder<Value> VALUE =
      new Builder<Value>() {
        @Override
        public Value build(Object obj) {
          List<Object> pair = (List<Object>) obj;
          if (pair.isEmpty()) return null;
          return new Value(LONG.build(pair.get(0)), DOUBLE.build(pair.get(1)));
        }
      };

  public static final Builder<List<Value>> VALUE_LIST =
      new Builder<List<Value>>() {
        @Override
        public List<Value> build(Object obj) {
          List<Object> list = (List<Object>) obj;
          return list.stream().map(VALUE::build).collect(Collectors.toList());
        }
      };
}
