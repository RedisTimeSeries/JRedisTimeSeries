package com.redislabs.redistimeseries;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class RangeParams {

  private Integer count;

  private byte[] align;

  private Aggregation aggregationType;
  private long timeBucket;

  public static RangeParams rangeParams() {
    return new RangeParams();
  }

  public RangeParams count(int count) {
    this.count = count;
    return this;
  }

  private RangeParams align(byte[] raw) {
    this.align = raw;
    return this;
  }

  public RangeParams align(long timestamp) {
    return align(Protocol.toByteArray(timestamp));
  }

  public RangeParams alignStart() {
    return align("start".getBytes());
  }

  public RangeParams alignEnd() {
    return align("end".getBytes());
  }

  public RangeParams aggregation(Aggregation aggregation, long timeBucket) {
    this.aggregationType = aggregation;
    this.timeBucket = timeBucket;
    return this;
  }

  public byte[][] getByteParams(String key, long from, long to) {
    List<byte[]> params = new ArrayList<>();
    params.add(SafeEncoder.encode(key));
    params.add(Protocol.toByteArray(from));
    params.add(Protocol.toByteArray(to));

    if (count != null) {
      params.add(Keyword.COUNT.getRaw());
      params.add(Protocol.toByteArray(count));
    }

    if (align != null) {
      params.add(Keyword.ALIGN.getRaw());
      params.add(align);
    }

    if (aggregationType != null) {
      params.add(Keyword.AGGREGATION.getRaw());
      params.add(aggregationType.getRaw());
      params.add(Protocol.toByteArray(timeBucket));
    }

    return params.toArray(new byte[params.size()][]);
  }
}
