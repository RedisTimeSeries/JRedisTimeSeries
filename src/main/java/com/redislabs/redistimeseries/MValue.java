package com.redislabs.redistimeseries;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;

public class MValue {

  private final String key;
  private final Map<String, String> labels;
  private final Value value;

  public MValue(String key, Map<String, String> labels, Value value) {
    this.key = key;
    this.labels = labels;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public Value getValue() {
    return value;
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

  public static final Builder<MValue> MVALUE =
      new Builder<MValue>() {
        @Override
        public MValue build(Object o) {
          List<Object> list = (List<Object>) o;
          return new MValue(
              BuilderFactory.STRING.build(list.get(0)),
              LABELS.build(list.get(1)),
              Value.VALUE.build(list.get(2)));
        }
      };

  public static final Builder<List<MValue>> MVALUE_LIST =
      new Builder<List<MValue>>() {
        @Override
        public List<MValue> build(Object o) {
          List<Object> list = (List<Object>) o;
          return list.stream().map(MVALUE::build).collect(Collectors.toList());
        }
      };
}
