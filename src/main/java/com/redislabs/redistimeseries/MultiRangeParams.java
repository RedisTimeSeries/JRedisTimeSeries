package com.redislabs.redistimeseries;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class MultiRangeParams {

  private long[] timestamps;
  private double[] values;

  private Integer count;

  private Aggregation aggregationType;
  private long timeBucket;

  private boolean withLabels;

  public static MultiRangeParams multiRangeParams() {
    return new MultiRangeParams();
  }

  public MultiRangeParams filterByTS(long... timestamps) {
    this.timestamps = timestamps;
    return this;
  }

  public MultiRangeParams filterByValues(double min, double max) {
    this.values = new double[] {min, max};
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

  public byte[][] getByteParams(long from, long to, String... filters) {
    List<byte[]> params = new ArrayList<>();
    params.add(Protocol.toByteArray(from));
    params.add(Protocol.toByteArray(to));

    if (timestamps != null) {
      params.add(Keyword.FILTER_BY_TS.getRaw());
      for (long ts : timestamps) {
        params.add(Protocol.toByteArray(ts));
      }
    }

    if (values != null) {
      params.add(Keyword.FILTER_BY_VALUE.getRaw());
      for (double value : values) {
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
