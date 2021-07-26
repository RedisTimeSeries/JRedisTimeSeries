package com.redislabs.redistimeseries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class AddParams {

  private Long retentionTime;
  private boolean uncompressed;
  private Long chunkSize;
  private DuplicatePolicy duplicatePolicy;
  private Map<String, String> labels;

  public static AddParams addParams() {
    return new AddParams();
  }

  public AddParams retentionTime(long retentionTime) {
    this.retentionTime = retentionTime;
    return this;
  }

  public AddParams uncompressed() {
    this.uncompressed = true;
    return this;
  }

  public AddParams chunkSize(long chunkSize) {
    this.chunkSize = chunkSize;
    return this;
  }

  public AddParams duplicatePolicy(DuplicatePolicy duplicatePolicy) {
    this.duplicatePolicy = duplicatePolicy;
    return this;
  }

  public AddParams label(String label, String value) {
    if (this.labels == null) this.labels = new HashMap<>();
    this.labels.put(label, value);
    return this;
  }

  public AddParams labels(String... labels) {
    final int len = labels.length;
    if ((len & 1) != 0) {
      throw new IllegalArgumentException();
    }
    Map<String, String> map = new HashMap<>(len >> 1);
    for (int i = 0; i < len; i += 2) {
      map.put(labels[i], labels[i + 1]);
    }
    return labels(map);
  }

  public AddParams labels(Map<String, String> labels) {
    this.labels = labels;
    return this;
  }

  public byte[][] getByteParams(String key, long timestamp, double value) {
    List<byte[]> params = new ArrayList<>();
    params.add(SafeEncoder.encode(key));
    params.add(Protocol.toByteArray(timestamp));
    params.add(Protocol.toByteArray(value));

    if (retentionTime != null) {
      params.add(Keyword.RETENTION.getRaw());
      params.add(Protocol.toByteArray(retentionTime));
    }
    if (uncompressed) {
      params.add(Keyword.UNCOMPRESSED.getRaw());
    }
    if (chunkSize != null) {
      params.add(Keyword.CHUNK_SIZE.getRaw());
      params.add(Protocol.toByteArray(chunkSize));
    }
    if (duplicatePolicy != null) {
      params.add(Keyword.DUPLICATE_POLICY.getRaw());
      params.add(duplicatePolicy.getRaw());
    }
    if (labels != null) {
      params.add(Keyword.LABELS.getRaw());
      for (Map.Entry<String, String> entry : labels.entrySet()) {
        params.add(SafeEncoder.encode(entry.getKey()));
        params.add(SafeEncoder.encode(entry.getValue()));
      }
    }
    return params.toArray(new byte[params.size()][]);
  }
}
