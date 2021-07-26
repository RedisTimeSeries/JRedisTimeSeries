package com.redislabs.redistimeseries;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class MultiRangeParams {

  private Integer count;

  private Aggregation aggregationType;
  private long timeBucket;

  private boolean withLabels;

  private final List<String> filters = new ArrayList<>();

  public static MultiRangeParams rangeParams() {
    return new MultiRangeParams();
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

  /**
   * To use multiple filter, use method chaining. E.g. {@code .filter(f_1)...filter(f_n)}
   *
   * @param filter
   * @return
   */
  public MultiRangeParams filter(String filter) {
    this.filters.add(filter);
    return this;
  }

  public MultiRangeParams filter(String... filters) {
    for (String filter : filters) {
      this.filter(filter);
    }
    return this;
  }

  public MultiRangeParams withLabels() {
    this.withLabels = true;
    return this;
  }

  public byte[][] getByteParams(long from, long to) {
    List<byte[]> params = new ArrayList<>();
    params.add(Protocol.toByteArray(from));
    params.add(Protocol.toByteArray(to));

    if (count != null) {
      params.add(Keyword.COUNT.getRaw());
    }

    if (aggregationType != null) {
      params.add(Keyword.AGGREGATION.getRaw());
      params.add(aggregationType.getRaw());
      params.add(Protocol.toByteArray(timeBucket));
    }

    if (withLabels) {
      params.add(Keyword.WITHLABELS.getRaw());
    }

    if (!filters.isEmpty()) {
      params.add(Keyword.FILTER.getRaw());
      filters.stream().map(SafeEncoder::encode).forEachOrdered(params::add);
    }

    return params.toArray(new byte[params.size()][]);
  }
}
