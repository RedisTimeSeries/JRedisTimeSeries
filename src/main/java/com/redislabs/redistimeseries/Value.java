package com.redislabs.redistimeseries;

public class Value {

  private final long time;
  private final double value;
  
  public Value(long time, double value) {
    this.time = time;
    this.value = value;
  }
  
  public long getTime() {
    return time;
  }
  public double getValue() {
    return value;
  }
}
