package com.redislabs.redistimeseries;

public class Measurement {
  private final String sourceKey;
  private final long timestamp;
  private final double value;

  public Measurement(String sourceKey, long timestamp, double value) {
    this.sourceKey = sourceKey;
    this.timestamp = timestamp;
    this.value = value;
  }

  public String getSourceKey() {
    return sourceKey;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public double getValue() {
    return value;
  }
}
