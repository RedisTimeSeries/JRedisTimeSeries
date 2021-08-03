package com.redislabs.redistimeseries;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class RangeParams {

  private long[] filterByTimestamps;
  private double[] filterByValues;

  private Integer count;

  private Aggregation aggregationType;
  private long timeBucket;

  public static RangeParams rangeParams() {
    return new RangeParams();
  }

  public RangeParams filterByTS(long... timestamps) {
    this.filterByTimestamps = timestamps;
    return this;
  }

  public RangeParams filterByValues(double min, double max) {
    this.filterByValues = new double[] {min, max};
    return this;
  }

  public RangeParams count(int count) {
    this.count = count;
    return this;
  }

  public RangeParams aggregation(Aggregation aggregation, long timeBucket) {
    this.aggregationType = aggregation;
    this.timeBucket = timeBucket;
    return this;
  }

  public byte[][] getByteParams(String key, Long from, Long to) {
    List<byte[]> params = new ArrayList<>();
    params.add(SafeEncoder.encode(key));

    if (from == null) {
      params.add("-".getBytes());
    } else {
      params.add(Protocol.toByteArray(from));
    }

    if (to == null) {
      params.add("+".getBytes());
    } else {
      params.add(Protocol.toByteArray(to));
    }

    if (filterByTimestamps != null) {
      params.add(Keyword.FILTER_BY_TS.getRaw());
      for (long ts : filterByTimestamps) {
        params.add(Protocol.toByteArray(ts));
      }
    }

    if (filterByValues != null) {
      params.add(Keyword.FILTER_BY_VALUE.getRaw());
      for (double value : filterByValues) {
        params.add(Protocol.toByteArray(value));
      }
    }

    if (count != null) {
      params.add(Keyword.COUNT.getRaw());
      params.add(Protocol.toByteArray(count));
    }

    if (aggregationType != null) {
      params.add(Keyword.AGGREGATION.getRaw());
      params.add(aggregationType.getRaw());
      params.add(Protocol.toByteArray(timeBucket));
    }

    return params.toArray(new byte[params.size()][]);
  }
}
