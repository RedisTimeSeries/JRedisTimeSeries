package com.redislabs.redistimeseries;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class MultiRangeParams {

  private long[] filterByTimestamps;
  private double[] filterByValues;

  private Integer count;

  private Aggregation aggregationType;
  private long timeBucket;

  private boolean withLabels;

  public static MultiRangeParams multiRangeParams() {
    return new MultiRangeParams();
  }

  public MultiRangeParams filterByTS(long... timestamps) {
    this.filterByTimestamps = timestamps;
    return this;
  }

  public MultiRangeParams filterByValues(double min, double max) {
    this.filterByValues = new double[] {min, max};
    return this;
  }

  public MultiRangeParams count(int count) {
    this.count = count;
    return this;
  }

  public MultiRangeParams aggregation(Aggregation aggregation, long timeBucket) {
    this.aggregationType = aggregation;
    this.timeBucket = timeBucket;
    return this;
  }

  public MultiRangeParams withLabels() {
    this.withLabels = true;
    return this;
  }

  public MultiRangeParams withLabels(boolean withLabels) {
    if (withLabels) {
      return withLabels();
    }
    return this;
  }

  public byte[][] getByteParams(Long from, Long to, String... filters) {
    List<byte[]> params = new ArrayList<>();

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

    if (withLabels) {
      params.add(Keyword.WITHLABELS.getRaw());
    }

    params.add(Keyword.FILTER.getRaw());
    for (String filter : filters) {
      params.add(SafeEncoder.encode(filter));
    }

    return params.toArray(new byte[params.size()][]);
  }
}
