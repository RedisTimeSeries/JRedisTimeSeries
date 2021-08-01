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
  private String[] selectedLabels;

  public static MultiRangeParams multiRangeParams() {
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

  /** WARNING: SELECTED_LABELS and WITHLABELS are mutually exclusive. */
  public MultiRangeParams selectedLabels(String... labels) {
    this.selectedLabels = labels;
    return this;
  }

  public byte[][] getByteParams(long from, long to, String... filters) {
    List<byte[]> params = new ArrayList<>();
    params.add(Protocol.toByteArray(from));
    params.add(Protocol.toByteArray(to));

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
    } else if (selectedLabels != null) {
      params.add(Keyword.SELECTED_LABELS.getRaw());
      for (String label : selectedLabels) {
        params.add(SafeEncoder.encode(label));
      }
    }

    params.add(Keyword.FILTER.getRaw());
    for (String filter : filters) {
      params.add(SafeEncoder.encode(filter));
    }

    return params.toArray(new byte[params.size()][]);
  }
}
