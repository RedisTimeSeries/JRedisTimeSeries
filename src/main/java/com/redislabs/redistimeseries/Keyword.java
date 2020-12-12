package com.redislabs.redistimeseries;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Keyword implements ProtocolCommand {
  RESET,
  FILTER,
  AGGREGATION,
  LABELS,
  RETENTION,
  TIMESTAMP,
  WITHLABELS,
  COUNT,
  UNCOMPRESSED,
  CHUNK_SIZE,
  DUPLICATE_POLICY,
  ON_DUPLICATE;

  private final byte[] raw;

  Keyword() {
    raw = SafeEncoder.encode(this.name());
  }

  @Override
  public byte[] getRaw() {
    return raw;
  }
}
