package com.redislabs.redistimeseries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class Range {
  private final String key;
  private final Map<String, String> labels;
  private final Value[] values;

  public Range(String key, Map<String, String> labels, Value[] values) {
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

  public Value[] getValues() {
    return values;
  }

  protected static Value[] parseRange(List<Object> range) {
    Value[] values = new Value[range.size()];

    for (int i = 0; i < values.length; ++i) {
      @SuppressWarnings("unchecked")
      List<Object> tuple = (List<Object>) range.get(i);
      values[i] = Value.parseValue(tuple);
    }
    return values;
  }

  private static Map<String, String> getLabelsStringStringMap(List<?> resLabels) {
    Map<String, String> rangeLabels = new HashMap<>(resLabels.size());
    for (Object resLabel : resLabels) {
      List<byte[]> label = (List<byte[]>) resLabel;
      rangeLabels.put(SafeEncoder.encode(label.get(0)), SafeEncoder.encode(label.get(1)));
    }
    return rangeLabels;
  }

  protected static Range[] parseRanges(List<?> result) {
    Range[] ranges = new Range[result.size()];
    for (int j = 0; j < ranges.length; ++j) {
      List<?> series = (List<?>) result.get(j);
      String resKey = SafeEncoder.encode((byte[]) series.get(0));
      List<?> resLabels = (List<?>) series.get(1);
      Map<String, String> rangeLabels = getLabelsStringStringMap(resLabels);
      Value[] values = parseRange((List<Object>) series.get(2));
      ranges[j] = new Range(resKey, rangeLabels, values);
    }
    return ranges;
  }

  protected static Range[] parseMget(List<?> result) {
    Range[] ranges = new Range[result.size()];
    for (int j = 0; j < ranges.length; ++j) {
      List<?> series = (List<?>) result.get(j);
      String resKey = SafeEncoder.encode((byte[]) series.get(0));
      List<?> resLabels = (List<?>) series.get(1);
      Map<String, String> rangeLabels = getLabelsStringStringMap(resLabels);
      List<?> tuple = (List<?>) series.get(2);
      Value[] values;
      if (tuple.isEmpty()) {
        values = new Value[0];
      } else {
        values = new Value[1];
        values[0] = Value.parseValue((List<Object>) tuple);
      }

      ranges[j] = new Range(resKey, rangeLabels, values);
    }
    return ranges;
  }

  protected static byte[][] multiRangeArgs(
      long from,
      long to,
      Aggregation aggregation,
      long timeBucket,
      boolean withLabels,
      Integer count,
      String[] filters) {
    byte[][] args =
        new byte
            [2
                + (filters == null ? 0 : 1 + filters.length)
                + (aggregation == null ? 0 : 3)
                + (withLabels ? 1 : 0)
                + (count == null ? 0 : 2)]
            [];
    int i = 0;
    args[i++] = Protocol.toByteArray(from);
    args[i++] = Protocol.toByteArray(to);
    if (aggregation != null) {
      args[i++] = Keyword.AGGREGATION.getRaw();
      args[i++] = aggregation.getRaw();
      args[i++] = Protocol.toByteArray(timeBucket);
    }
    if (withLabels) {
      args[i++] = Keyword.WITHLABELS.getRaw();
    }
    if (count != null) {
      args[i++] = Keyword.COUNT.getRaw();
      args[i++] = Protocol.toByteArray(count.intValue());
    }

    if (filters != null) {
      args[i++] = Keyword.FILTER.getRaw();
      for (String label : filters) {
        args[i++] = SafeEncoder.encode(label);
      }
    }
    return args;
  }
}
