package com.redislabs.redistimeseries.information;

import com.redislabs.redistimeseries.Aggregation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.util.SafeEncoder;

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

  public static Info parseInfoReply(List<Object> resp) {
    Map<String, Long> properties = new HashMap<>();
    Map<String, String> labels = new HashMap<>();
    Map<String, Rule> rules = new HashMap<>();
    for (int i = 0; i < resp.size(); i += 2) {
      String prop = SafeEncoder.encode((byte[]) resp.get(i));
      Object value = resp.get(i + 1);
      if (value instanceof Long) {
        properties.put(prop, (Long) value);
      } else {
        if (prop.equals("labels")) {
          List<List<byte[]>> labelsList = (List<List<byte[]>>) value;
          for (List<byte[]> labelBytes : labelsList) {
            labels.put(
                SafeEncoder.encode((byte[]) labelBytes.get(0)),
                SafeEncoder.encode((byte[]) labelBytes.get(1)));
          }
        } else if (prop.equals("rules")) {
          List<List<Object>> rulesList = (List<List<Object>>) value;
          for (List<Object> ruleBytes : rulesList) {
            String target = SafeEncoder.encode((byte[]) ruleBytes.get(0));
            String agg = SafeEncoder.encode((byte[]) ruleBytes.get(2));
            rules.put(target, new Rule(target, (Long) ruleBytes.get(1), Aggregation.valueOf(agg)));
          }
        }
      }
    }
    return new Info(properties, labels, rules);
  }
}
