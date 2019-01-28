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
  
  @Override
  public boolean equals(Object o) {
    if(!(o instanceof Value))
        return false;
    
    Value other = (Value)o;
    return other.time == this.time && other.value == this.value;
  }
  
  @Override
  public int hashCode() {
    return Long.hashCode(time ) ^ Double.hashCode(value);
  }
}
