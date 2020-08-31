package com.redislabs.redistimeseries;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    for(int i=0; i<values.length ; ++i) {
      @SuppressWarnings("unchecked") List<Object> touple = (List<Object>)range.get(i);
      values[i] = new Value((Long)touple.get(0), Double.parseDouble(SafeEncoder.encode((byte[])touple.get(1))));
    }
    return values;
  }

  protected static Range[] parseRanges(List<?> result) {
    Range[] ranges = new Range[result.size()];
    for(int j=0; j<ranges.length; ++j) {
      List<?> series = (List<?>)result.get(j);
      String resKey = SafeEncoder.encode((byte[])series.get(0));
      List<?> resLabels = (List<?>)series.get(1);
      Map<String, String> rangeLabels = new HashMap<>();
      for (Object resLabel : resLabels) {
        List<byte[]> label = (List<byte[]>) resLabel;
        rangeLabels.put(SafeEncoder.encode(label.get(0)), SafeEncoder.encode(label.get(1)));
      }
      Value[] values = parseRange((List<Object>) series.get(2));
      ranges[j] = new Range(resKey, rangeLabels, values);
    }
    return ranges;
  }

  protected static byte[][] multiRangeArgs(long from, long to, Aggregation aggregation, long timeBucket, boolean withLabels, int count, String[] filters) {
    byte[][] args = new byte[3 + (filters==null?0:filters.length) + (aggregation==null?0:3) + (withLabels?1:0) + (count==Integer.MAX_VALUE?0:2)][];
    int i=0;
    args[i++] = Protocol.toByteArray(from);
    args[i++] = Protocol.toByteArray(to);
    if(aggregation!= null) {
      args[i++] = Keyword.AGGREGATION.getRaw();
      args[i++] = aggregation.getRaw();
      args[i++] = Protocol.toByteArray(timeBucket);
    }
    if(withLabels) {
      args[i++] = Keyword.WITHLABELS.getRaw();
    }
    if(count != Integer.MAX_VALUE) {
      args[i++] = Keyword.COUNT.getRaw();
      args[i++] = Protocol.toByteArray(count);
    }

    args[i++] = Keyword.FILTER.getRaw();
    if(filters != null) {
      for(String label : filters) {
        args[i++] = SafeEncoder.encode(label);
      }
    }
    return args;
  }
}
