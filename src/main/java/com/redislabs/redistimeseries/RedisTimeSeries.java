package com.redislabs.redistimeseries;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.redislabs.redistimeseries.information.Info;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;


public class RedisTimeSeries {

  private static final byte[] STAR = SafeEncoder.encode("*");

  private final Pool<Jedis> pool;


  /**
   * Create a new RedisTimeSeries client with default connection to local host
   */
  public RedisTimeSeries() {
    this("localhost", 6379);
  }

  /**
   * Create a new RedisTimeSeries client
   *
   * @param host      the redis host
   * @param port      the redis pot
   */
  public RedisTimeSeries(String host, int port) {
    this(host, port, 500, 100);
  }

  /**
   * Create a new RedisTimeSeries client
   *
   * @param host      the redis host
   * @param port      the redis pot
   */
  public RedisTimeSeries(String host, int port, int timeout, int poolSize) {
    this(host, port, timeout, poolSize, null);
  }

  /**
   * Create a new RedisTimeSeries client
   *
   * @param host      the redis host
   * @param port      the redis pot
   * @param password  the password for authentication in a password protected Redis server
   */
  public RedisTimeSeries(String host, int port, int timeout, int poolSize, String password) {
    this(new JedisPool(initPoolConfig(poolSize), host, port, timeout, password));
  }

  public RedisTimeSeries(Pool<Jedis> pool) {
    this.pool = pool;
  }


  /**
   * TS.CREATE key
   *
   * @param key
   * @return
   */
  public boolean create(String key){
    return create(key, -1, false, null);
  }


  /**
   * TS.CREATE key [RETENTION retentionTime]
   *
   * @param key
   * @param retentionTime
   * @return
   */
  public boolean create(String key, long retentionTime){
    return create(key, retentionTime, false, null);
  }

  /**
   * TS.CREATE key [LABELS field value..]
   *
   * @param key
   * @param labels
   * @return
   */
  public boolean create(String key, Map<String, String> labels){
    return create(key, -1, false, labels);
  }


  /**
   * TS.CREATE key [RETENTION retentionTime] [LABELS field value..]
   *
   * @param key
   * @param retentionTime
   * @param labels
   * @return
   */
  public boolean create(String key, long retentionTime, Map<String, String> labels){
    return create(key, retentionTime, false, labels);
  }

  /**
   * TS.CREATE key [RETENTION retentionTime] [UNCOMPRESSED] [LABELS field value..]
   *
   * @param key
   * @param retentionTime
   * @param uncompressed
   * @param labels
   * @return
   */
  public boolean create(String key, long retentionTime, boolean uncompressed, Map<String, String> labels){
    try (Jedis conn = getConnection()) {
      byte[][] args = tsCreateArgs(key, retentionTime, uncompressed, labels);
      return sendCommand(conn, Command.CREATE, args).getStatusCodeReply().equals("OK");
    }
  }

  private static byte[][] tsCreateArgs(String key, long retentionTime, boolean uncompressed, Map<String, String> labels) {
    byte[][] args = new byte[1 + (labels==null ? 0 : 2*labels.size()+1) + (retentionTime>=0 ? 2 : 0 ) + (uncompressed?1:0)][];
    int i=0;

    args[i++] = SafeEncoder.encode(key);
    if(retentionTime>=0) {
      args[i++] = Keyword.RETENTION.getRaw();
      args[i++] = Protocol.toByteArray(retentionTime);
    }
    if(uncompressed) {
      args[i++] = Keyword.UNCOMPRESSED.getRaw();
    }

    if(labels != null) {
      args[i++] = Keyword.LABELS.getRaw();
      for(Entry<String, String> e : labels.entrySet()) {
        args[i++] = SafeEncoder.encode(e.getKey());
        args[i++] = SafeEncoder.encode(e.getValue());
      }
    }
    return args;
  }

  /**
   * TS.ALTER key [LABELS label value..]
   * @param key
   * @param labels
   * @return
   */
  public boolean alter(String key, Map<String, String> labels) {
    return alter(key,-1, labels );
  }

  /**
   * TS.ALTER key [RETENTION retentionTime] [LABELS label value..]
   * @param key
   * @param retentionTime
   * @param labels
   * @return
   */
  public boolean alter(String key, long retentionTime, Map<String, String> labels) {
    try (Jedis conn = getConnection()) {
      byte[][] args = tsAlterArgs(key, retentionTime, labels);
      return sendCommand(conn, Command.ALTER, args).getStatusCodeReply().equals("OK");
    }
  }

  private static byte[][] tsAlterArgs(String key, long retentionTime, Map<String, String> labels) {
    byte[][] args = new byte[1 + (retentionTime>=0 ? 2 : 0 ) + (labels==null ? 0 : 2*labels.size()+1)][];
    int i=0;
    args[i++] = SafeEncoder.encode(key);
    if (retentionTime>=0){
      args[i++] = Keyword.RETENTION.getRaw();
      args[i++] = Protocol.toByteArray(retentionTime);
    }
    if(labels != null) {
      args[i++] = Keyword.LABELS.getRaw();
      for(Entry<String, String> e : labels.entrySet()) {
        args[i++] = SafeEncoder.encode(e.getKey());
        args[i++] = SafeEncoder.encode(e.getValue());
      }
    }
    return args;
  }

  /**
   * TS.CREATERULE sourceKey destKey AGGREGATION aggType retentionTime
   *
   * @param sourceKey
   * @param aggregation
   * @param bucketSize
   * @param destKey
   * @return
   */
  public boolean createRule(String sourceKey, Aggregation aggregation, long bucketSize, String destKey) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.CREATE_RULE, SafeEncoder.encode(sourceKey), SafeEncoder.encode(destKey),
          Keyword.AGGREGATION.getRaw(), aggregation.getRaw(), Protocol.toByteArray(bucketSize))
          .getStatusCodeReply().equals("OK");
    }
  }

  /**
   * TS.DELETERULE SOURCE_KEY DEST_KEY
   *
   * @param sourceKey
   * @param destKey
   * @return
   */
  public boolean deleteRule(String sourceKey, String destKey) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.DELETE_RULE, SafeEncoder.encode(sourceKey), SafeEncoder.encode(destKey))
          .getStatusCodeReply().equals("OK");
    }
  }

  /**
   * TS.ADD key * value
   *
   * @param sourceKey
   * @param value
   * @return
   */
  public long add(String sourceKey, double value) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.ADD, SafeEncoder.encode(sourceKey),
          STAR, Protocol.toByteArray(value))
          .getIntegerReply();
    }
  }

  /**
   * TS.ADD key timestamp value
   *
   * @param sourceKey
   * @param timestamp
   * @param value
   * @return
   */
  public long add(String sourceKey, long timestamp, double value) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.ADD, SafeEncoder.encode(sourceKey),
          timestamp>0 ? Protocol.toByteArray(timestamp) : STAR, Protocol.toByteArray(value))
          .getIntegerReply();
    }
  }

  /**
   * TS.ADD key timestamp value [RETENTION retentionTime]
   *
   * @param sourceKey
   * @param timestamp
   * @param value
   * @param retentionTime
   * @return
   */
  public long add(String sourceKey, long timestamp, double value, long retentionTime) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.ADD, SafeEncoder.encode(sourceKey), timestamp>0 ? Protocol.toByteArray(timestamp) : STAR,
          Protocol.toByteArray(value), Keyword.RETENTION.getRaw(), Protocol.toByteArray(retentionTime)).getIntegerReply();
    }
  }

  /**
   * TS.ADD key timestamp value [RETENTION retentionTime] [LABELS field value..]
   *
   * @param sourceKey
   * @param timestamp
   * @param value
   * @param labels
   * @return
   */
  public long add(String sourceKey, long timestamp, double value, Map<String, String> labels) {
    return add(sourceKey, timestamp, value, -1, false, labels);
  }

  /**
   * TS.ADD key timestamp value [RETENTION retentionTime] [LABELS field value..]
   *
   * @param sourceKey
   * @param timestamp
   * @param value
   * @param retentionTime
   * @param labels
   * @return
   */
  public long add(String sourceKey, long timestamp, double value, long retentionTime, Map<String, String> labels) {
    return add(sourceKey, timestamp, value, retentionTime, false, labels);
  }

  /**
   * TS.ADD key timestamp value [RETENTION retentionTime] UNCOMPRESSED [LABELS field value..]
   *
   * @param sourceKey
   * @param timestamp
   * @param value
   * @param retentionTime
   * @param uncompressed
   * @param labels
   * @return
   */
  public long add(String sourceKey, long timestamp, double value, long retentionTime, boolean uncompressed, Map<String, String> labels) {
    try (Jedis conn = getConnection()) {
      byte[][] args = tsAddArgs(sourceKey, timestamp, value, retentionTime, uncompressed, labels);
      return sendCommand(conn, Command.ADD, args).getIntegerReply();
    }
  }

  private static byte[][] tsAddArgs(String sourceKey, long timestamp, double value, long retentionTime, boolean uncompressed, Map<String, String> labels) {
    byte[][] args = new byte[3 + (retentionTime>=0 ? 2 : 0 ) + (labels==null ? 0 : 2*labels.size()+1) + (uncompressed?1:0)][];
    int i=0;

    args[i++] = SafeEncoder.encode(sourceKey);
    args[i++] = timestamp>0 ? Protocol.toByteArray(timestamp) : STAR;
    args[i++] = Protocol.toByteArray(value);
    if(retentionTime>=0){
      args[i++] = Keyword.RETENTION.getRaw();
      args[i++] = Protocol.toByteArray(retentionTime);
    }
    if(uncompressed) {
      args[i++] = Keyword.UNCOMPRESSED.getRaw();
    }
    if(labels != null) {
      args[i++] = Keyword.LABELS.getRaw();
      for(Entry<String, String> e : labels.entrySet()) {
        args[i++] = SafeEncoder.encode(e.getKey());
        args[i++] = SafeEncoder.encode(e.getValue());
      }
    }
    return args;
  }

  /**
   * TS.MADD key timestamp value [key timestamp value ...]
   *
   * @param measurements
   * @return
   */
  public List<Object> madd(Measurement ...measurements) {

    try (Jedis conn = getConnection()) {
      byte[][] args = new byte[measurements.length*3][];
      for(int i=0; i<measurements.length; ++i) {
        int argsIndx = i*3;
        args[argsIndx] = SafeEncoder.encode(measurements[i].getSourceKey());
        long timestamp = measurements[i].getTimestamp();
        args[argsIndx+1] = timestamp>0 ? Protocol.toByteArray(timestamp) : STAR;
        args[argsIndx+2] = Protocol.toByteArray(measurements[i].getValue());
      }
      return sendCommand(conn, Command.MADD, args).getObjectMultiBulkReply();
    }
  }

  /**
   * @param command Should be {@link Command#RANGE} or {@link Command#REVRANGE}
   * @param args
   * @return
   */
  private Value[] range(Command command, byte[]... args) {
    try (Jedis conn = getConnection()) {
      List<Object> range = sendCommand(conn, command, args)
          .getObjectMultiBulkReply();

      return Range.parseRange(range);
    }
  }


  /**
   * TS.RANGE key fromTimestamp toTimestamp
   *
   * @param key
   * @param from
   * @param to
   * @return
   */
  public Value[] range(String key, long from, long to) {  
      return range(Command.RANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to));
  }

  /**
   * TS.RANGE key fromTimestamp toTimestamp [COUNT count]
   *
   * @param key
   * @param from
   * @param to
   * @return
   */
  public Value[] range(String key, long from, long to, int count) {  
      return range(Command.RANGE,SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to),
          Keyword.COUNT.getRaw(), Protocol.toByteArray(count));
  }

  /**
   * TS.RANGE key fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket]
   *
   * @param key
   * @param from
   * @param to
   * @return
   */
  public Value[] range(String key, long from, long to, Aggregation aggregation, long timeBucket) {
      return range(Command.RANGE,SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to), 
          Keyword.AGGREGATION.getRaw(), aggregation.getRaw(), Protocol.toByteArray(timeBucket));
  }

  /**
   * TS.RANGE key fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregationType timeBucket]
   *
   * @param key
   * @param from
   * @param to
   * @param count
   * @return
   */
  public Value[] range(String key, long from, long to, Aggregation aggregation, long timeBucket, int count) {
    return range(Command.RANGE,SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to),
            Keyword.AGGREGATION.getRaw(), aggregation.getRaw(), Protocol.toByteArray(timeBucket),
            Keyword.COUNT.getRaw(), Protocol.toByteArray(count));
  }

  /**
   * TS.REVRANGE key fromTimestamp toTimestamp
   * 
   * @param key
   * @param from
   * @param to
   * @return
   */
  public Value[] revrange(String key, long from, long to) {  
      return range(Command.REVRANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to));
  }
  
  /**
   * TS.REVRANGE key fromTimestamp toTimestamp [COUNT count] 
   * 
   * @param key
   * @param from
   * @param to
   * @param count
   * @return
   */
  public Value[] revrange(String key, long from, long to, int count) {  
      return range(Command.REVRANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to),
          Keyword.COUNT.getRaw(), Protocol.toByteArray(count));
  }

  /**
   * TS.REVRANGE key fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket]
   * 
   * @param key
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @return
   */
  public Value[] revrange(String key, long from, long to, Aggregation aggregation, long timeBucket) {
      return range(Command.REVRANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to), 
          Keyword.AGGREGATION.getRaw(), aggregation.getRaw(), Protocol.toByteArray(timeBucket));
  }
  
  /**
   * TS.REVRANGE key fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket] [COUNT count] 
   * 
   * @param key
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param count
   * @return
   */
  public Value[] revrange(String key, long from, long to, Aggregation aggregation, long timeBucket, int count) {
      return range(Command.REVRANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to), 
          Keyword.AGGREGATION.getRaw(), aggregation.getRaw(), Protocol.toByteArray(timeBucket), 
          Keyword.COUNT.getRaw(), Protocol.toByteArray(count));
  }


  /**
   * TS.MRANGE fromTimestamp toTimestamp FILTER filter.
   * </br>
   * Similar to calling <code>mrange(from, to, null, 0, false, Integer.MAX_VALUE, filters)</code>
   *
   * @param from
   * @param to
   * @param filters
   * @return
   */
  public Range[] mrange(long from, long to, String... filters) {
    return mrange(Command.MRANGE,from, to, null /*aggregation*/, 0 /*timeBucket*/, false /*withLabels*/, Integer.MAX_VALUE /*count*/, filters);
  }

  /**
   * TS.MRANGE fromTimestamp toTimestamp [COUNT count] FILTER filter.
   * </br>
   * Similar to calling <code>mrange(from, to, null, 0, false, Integer.MAX_VALUE, filters)</code>
   *
   * @param from
   * @param to
   * @param count
   * @param filters
   * @return
   */
  public Range[] mrange(long from, long to, int count, String... filters) {
    return mrange(Command.MRANGE,from, to, null /*aggregation*/, 0 /*timeBucket*/, false /*withLabels*/, count, filters);
  }

  /**
   * TS.MRANGE fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket] FILTER filter.
   * </br>
   * Similar to calling <code>mrange(from, to, aggregation, retentionTime, false, Integer.MAX_VALUE, filters)</code>
   *
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param filters
   * @return
   */
  public Range[] mrange(long from, long to, Aggregation aggregation, long timeBucket, String... filters) {
    return mrange(Command.MRANGE, from, to, aggregation, timeBucket, false, Integer.MAX_VALUE /*count*/, filters);
  }

  /**
   * TS.MRANGE fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket] FILTER filter.
   * </br>
   * Similar to calling <code>mrange(from, to, aggregation, retentionTime, false, Integer.MAX_VALUE, filters)</code>
   *
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param filters
   * @return
   */
  public Range[] mrange(long from, long to, Aggregation aggregation, long timeBucket, boolean withLabels, String... filters) {
    return mrange(Command.MRANGE, from, to, aggregation, timeBucket, withLabels, Integer.MAX_VALUE /*count*/, filters);
  }

  /**
   * TS.MRANGE fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregationType timeBucket] FILTER filter.
   * </br>
   * Similar to calling <code>mrange(from, to, aggregation, retentionTime, false, Integer.MAX_VALUE, filters)</code>
   *
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param filters
   * @param count
   * @return
   */
  public Range[] mrange(long from, long to, Aggregation aggregation, long timeBucket, boolean withLabels, int count, String... filters) {
    return mrange(Command.MRANGE, from, to, aggregation, timeBucket, withLabels, count, filters);
  }

  /**
   * TS.MRANGE/TS.MREVRANGE fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregationType timeBucket] [WITHLABELS] FILTER filter..
   *
   * @param command Should be {@link Command#MRANGE} or {@link Command#MREVRANGE}
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param withLabels <code>true</code> if the labels should be returned for each range
   * @param count
   * @param filters
   * @return
   */
  private Range[] mrange(Command command, long from, long to, Aggregation aggregation, long timeBucket, boolean withLabels, int count, String... filters) {
    try (Jedis conn = getConnection()) {
      byte[][] args = Range.multiRangeArgs(from, to, aggregation, timeBucket, withLabels, count, filters);
      List<?> result = sendCommand(conn, command, args).getObjectMultiBulkReply();
      return Range.parseRanges(result);
    }
  }

  /**
   * TS.MREVRANGE fromTimestamp toTimestamp FILTER filter.
   * </br>
   * Similar to calling <code>mrevrange(from, to, null, 0, false, Integer.MAX_VALUE, filters)</code>
   * 
   * @param from
   * @param to
   * @param filters
   * @return
   */
  public Range[] mrevrange(long from, long to, String... filters) {
    return mrevrange(from, to, null /*aggregation*/, 0 /*timeBucket*/, filters);
  }
  
  /**
   * TS.MREVRANGE fromTimestamp toTimestamp [COUNT count] FILTER filter.
   * </br>
   * Similar to calling <code>mrevrange(from, to, null, 0, false, Integer.MAX_VALUE, filters)</code>
   * 
   * @param from
   * @param to
   * @param count
   * @param filters
   * @return
   */
  public Range[] mrevrange(long from, long to, int count, String... filters) {
    return mrevrange(from, to, null /*aggregation*/, 0 /*timeBucket*/, false /*withLabels*/, count, filters);
  }
  
  /**
   * TS.MREVRANGE fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket] FILTER filter.
   * </br>
   * Similar to calling <code>mrevrange(from, to, aggregation, retentionTime, false, Integer.MAX_VALUE, filters)</code>
   * 
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param filters
   * @return
   */
  public Range[] mrevrange(long from, long to, Aggregation aggregation, long timeBucket, String... filters) {
    return mrevrange(from, to, aggregation, timeBucket, false /*withLabels*/, filters);
  }
  
  /**
   * TS.MREVRANGE fromTimestamp toTimestamp [AGGREGATION aggregationType timeBucket] FILTER filter.
   * </br>
   * Similar to calling <code>mrevrange(from, to, aggregation, retentionTime, false, Integer.MAX_VALUE, filters)</code>
   * 
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param filters
   * @return
   */
  public Range[] mrevrange(long from, long to, Aggregation aggregation, long timeBucket, boolean withLabels, String... filters) {
    return mrevrange(from, to, aggregation, timeBucket, withLabels, Integer.MAX_VALUE /*count*/, filters);
  }

  /**
   * TS.MREVRANGE fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregationType timeBucket] [WITHLABELS] FILTER filter..
   * 
   * @param from
   * @param to
   * @param aggregation
   * @param timeBucket
   * @param withLabels <code>true</code> if the labels should be returned for each range
   * @param count
   * @param filters
   * @return
   */
  public Range[] mrevrange(long from, long to, Aggregation aggregation, long timeBucket, boolean withLabels, int count, String... filters) {
    return mrange(Command.MREVRANGE, from, to, aggregation, timeBucket, withLabels, count, filters);
  }
  

  /**
   * TS.GET key
   *
   * @param key
   * @return
   */
  public Value get(String key) {
    try (Jedis conn = getConnection()) {
      List<?> touple = sendCommand(conn, Command.GET, SafeEncoder.encode(key)).getObjectMultiBulkReply();
      if(touple.isEmpty()) {
        return null;
      }
      return new Value((Long)touple.get(0), Double.parseDouble(SafeEncoder.encode((byte[])touple.get(1))));
    }
  }

  /**
   * TS.MGET [WITHLABELS] FILTER filter...
   *
   * @param withLabels
   * @param filters
   * @return
   */
  public Range[] mget(boolean withLabels, String... filters) {
    try (Jedis conn = getConnection()) {
      byte[][] args = new byte[1 + (withLabels?1:0) + (filters==null?0:filters.length)][];
      int i=0;

      if(withLabels) {
        args[i++] = Keyword.WITHLABELS.getRaw();
      }

      args[i++] = Keyword.FILTER.getRaw();
      if(filters != null) {
        for(String label : filters) {
          args[i++] = SafeEncoder.encode(label);
        }
      }

      List<?> result = sendCommand(conn, Command.MGET, args).getObjectMultiBulkReply();
      return Range.parseMget(result);
    }
  }

  /**
   * TS.INCRBY key value
   *
   * @param key
   * @param value
   * @return
   */
  public long incrBy(String key, int value) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.INCRBY, SafeEncoder.encode(key), Protocol.toByteArray(value))
          .getIntegerReply();
    }
  }

  /**
   * TS.INCRBY key value [TIMESTAMP timestamp]
   *
   * @param key
   * @param value
   * @param timestamp
   * @return
   */
  public long incrBy(String key, int value, long timestamp) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.INCRBY, SafeEncoder.encode(key), Protocol.toByteArray(value),
          Keyword.TIMESTAMP.getRaw(), Protocol.toByteArray(timestamp))
          .getIntegerReply();
    }
  }

  /**
   * TS.DECRBY key value
   *
   * @param key
   * @param value
   * @return
   */
  public long decrBy(String key, int value) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.DECRBY, SafeEncoder.encode(key), Protocol.toByteArray(value))
          .getIntegerReply();
    }
  }

  /**
   * TS.DECRBY key value [TIMESTAMP timestamp]
   *
   * @param key
   * @param value
   * @param timestamp
   * @return
   */
  public long decrBy(String key, int value, long timestamp) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.DECRBY, SafeEncoder.encode(key), Protocol.toByteArray(value),
          Keyword.TIMESTAMP.getRaw(), Protocol.toByteArray(timestamp))
          .getIntegerReply();
    }
  }

  /**
   * TS.QUERYINDEX filter...
   */

  public String[] queryIndex(String... filters) {
    try (Jedis conn = getConnection()) {
      List<String> result = sendCommand(conn, Command.QUERYINDEX, SafeEncoder.encodeMany(filters)).getMultiBulkReply();
      return result.toArray(new String[result.size()]);
    }
  }

  /**
   * TS.INFO key
   *
   * @param key
   * @return
   */
  public Info info(String key) {
    try (Jedis conn = getConnection()) {
      List<Object> resp = sendCommand(conn, Command.INFO, SafeEncoder.encode(key))
          .getObjectMultiBulkReply();
      return Info.parseInfoReply(resp);
    }
  }

  private Jedis getConnection() {
    return pool.getResource();
  }

  private BinaryClient sendCommand(Jedis conn, Command command, byte[]... args) {
    BinaryClient client = conn.getClient();
    client.sendCommand(command, args);
    return client;
  }

  /**
   * Constructs JedisPoolConfig object.
   *
   * @param poolSize size of the JedisPool
   * @return {@link JedisPoolConfig} object with a few default settings
   */
  private static JedisPoolConfig initPoolConfig(int poolSize) {
    JedisPoolConfig conf = new JedisPoolConfig();
    conf.setMaxTotal(poolSize);
    conf.setTestOnBorrow(false);
    conf.setTestOnReturn(false);
    conf.setTestOnCreate(false);
    conf.setTestWhileIdle(false);
    conf.setMinEvictableIdleTimeMillis(60000);
    conf.setTimeBetweenEvictionRunsMillis(30000);
    conf.setNumTestsPerEvictionRun(-1);
    conf.setFairness(true);

    return conf;
  }
}
