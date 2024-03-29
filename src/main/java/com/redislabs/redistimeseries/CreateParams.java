package com.redislabs.redistimeseries;

import java.util.List;
import java.util.Map;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class CreateParams {

  private Long retentionTime;
  private boolean uncompressed;
  private Long chunkSize;
  private DuplicatePolicy duplicatePolicy;
  private Map<String, String> labels;

  public static CreateParams createParams() {
    return new CreateParams();
  }

  public CreateParams() {}

  public CreateParams retentionTime(long retentionTime) {
    this.retentionTime = retentionTime;
    return this;
  }

  public CreateParams uncompressed() {
    this.uncompressed = true;
    return this;
  }

  public CreateParams chunkSize(long chunkSize) {
    this.chunkSize = chunkSize;
    return this;
  }

  public CreateParams duplicatePolicy(DuplicatePolicy duplicatePolicy) {
    this.duplicatePolicy = duplicatePolicy;
    return this;
  }

  public CreateParams labels(Map<String, String> labels) {
    this.labels = labels;
    return this;
  }

  public void addOptionalParams(List<byte[]> params) {
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
  }
}
