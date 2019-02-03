package com.redislabs.redistimeseries;

import java.util.Map;

public class Range {
  private final String key;
  private final Map<String, String> lables;
  private final Value[] values;
  
  public Range(String key, Map<String, String> labels, Value[] values) {
    this.key = key;
    this.lables = labels;
    this.values = values;
  }

  public String getKey() {
    return key;
  }

  public Map<String, String> getLables() {
    return lables;
  }

  public Value[] getValues() {
    return values;
  }

}
