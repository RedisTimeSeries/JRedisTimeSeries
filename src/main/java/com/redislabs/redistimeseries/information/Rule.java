package com.redislabs.redistimeseries.information;

import com.redislabs.redistimeseries.Aggregation;

public class Rule {
  private final String target;
  private final long value;
  private final Aggregation aggregation;

  public Rule(String target, long value, Aggregation aggregation) {
    this.target = target;
    this.value = value;
    this.aggregation = aggregation;
  }

  public String getTarget() {
    return target;
  }

  public long getValue() {
    return value;
  }

  public Aggregation getAggregation() {
    return aggregation;
  }
}
