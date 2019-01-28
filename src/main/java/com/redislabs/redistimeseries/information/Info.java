package com.redislabs.redistimeseries.information;

import java.util.Map;

public class Info {

  private final Map<String, Long> properties;
  private final Map<String, String> labels;
  private final Map<String, Rule> rules;

  public Info(Map<String, Long> properties, Map<String, String> labels, Map<String, Rule> rules) {
    this.properties = properties;
    this.labels = labels;
    this.rules = rules;
  }
      
  public Long getProperty(String property) {
    return properties.get(property);
  }
  
  public String getLabel(String label) {
    return labels.get(label);
  }

  public Rule getRule(String rule) {
    return rules.get(rule);
  }
}
