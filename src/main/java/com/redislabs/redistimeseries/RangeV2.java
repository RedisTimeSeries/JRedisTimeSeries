package com.redislabs.redistimeseries;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;

public class RangeV2 {

  private final String key;
  private final Map<String, String> labels;
  private final List<Value> values;

  public RangeV2(String key, Map<String, String> labels, List<Value> values) {
    this.key = key;
    this.labels = labels;
    this.values = values;
  }

  public String getKey() {
    return key;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public List<Value> getValues() {
    return values;
  }

  private static final Builder<Map<String, String>> LABELS =
      new Builder<Map<String, String>>() {
        @Override
        public Map<String, String> build(Object o) {
          List<Object> list = (List<Object>) o;
          return list.stream()
              .map(BuilderFactory.STRING_LIST::build)
              .collect(Collectors.toMap(l -> l.get(0), l -> l.get(1)));
        }
      };

  private static final Builder<RangeV2> RANGE =
      new Builder<RangeV2>() {
        @Override
        public RangeV2 build(Object o) {
          List<Object> list = (List<Object>) o;
          return new RangeV2(
              BuilderFactory.STRING.build(list.get(0)),
              LABELS.build(list.get(1)),
              Value.VALUE_LIST.build(list.get(2)));
        }
      };

  public static final Builder<List<RangeV2>> RANGE_LIST =
      new Builder<List<RangeV2>>() {
        @Override
        public List<RangeV2> build(Object o) {
          List<Object> list = (List<Object>) o;
          return list.stream().map(RANGE::build).collect(Collectors.toList());
        }
      };
}
