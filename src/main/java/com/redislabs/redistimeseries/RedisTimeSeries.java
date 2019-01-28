package com.redislabs.redistimeseries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

public class RedisTimeSeries {

  private final Pool<Jedis> pool;
  
  public RedisTimeSeries(Pool<Jedis> pool) {
    this.pool = pool;
  }
  
  /**
   * TS.CREATE KEY [retentionSecs] [maxSamplesPerChunk]
   * 
   * @param key
   * @param retentionSecs
   * @param maxSamplesPerChunk
   * @return
   */
  public boolean create(String key, long retentionSecs, long maxSamplesPerChunk){
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.CREATE, SafeEncoder.encode(key), 
          Protocol.toByteArray(retentionSecs), Protocol.toByteArray(maxSamplesPerChunk))
      .getStatusCodeReply().equals("OK");
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }
  }
  

  /**
   * TS.CREATERULE SOURCE_KEY AGG_TYPE BUCKET_SIZE_SEC DEST_KEY
   * 
   * @param sourceKey
   * @param aggregation
   * @param bucketSize
   * @param destKey
   * @return
   */
  public boolean createRule(String sourceKey, Aggregation aggregation, long bucketSize, String destKey) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.CREATE_RULE, SafeEncoder.encode(sourceKey), aggregation.getRaw(), 
          Protocol.toByteArray(bucketSize),  SafeEncoder.encode(destKey))
      .getStatusCodeReply().equals("OK");
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
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
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }

  }
  
  /**
   * TS.ADD key TIMESTAMP value
   * 
   * @param sourceKey
   * @param timestamp
   * @param value
   * @return
   */
  public boolean add(String sourceKey, long timestamp, double value) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.ADD, SafeEncoder.encode(sourceKey),  
          Protocol.toByteArray(timestamp),  Protocol.toByteArray(value))
      .getStatusCodeReply().equals("OK");
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }
  }
  
  /**
   * TS.RANGE key FROM_TIMESTAMP TO_TIMESTAMP [aggregationType] [bucketSizeSeconds]
   * 
   * @param key
   * @param from
   * @param to
   * @param aggregation
   * @param bucketSizeSeconds
   * @return
   */
  public Value[] range(String key, long from, long to, Aggregation aggregation, long bucketSizeSeconds) {
    try (Jedis conn = getConnection()) {
      List<Object> range = sendCommand(conn, Command.RANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), 
          Protocol.toByteArray(to), aggregation.getRaw(), Protocol.toByteArray(bucketSizeSeconds))
      .getObjectMultiBulkReply();

      Value[] values = new Value[range.size()];
      
      for(int i=0; i<values.length ; ++i) {
        List<Object> touple = (List<Object>)range.get(i);
        values[i] = new Value((Long)touple.get(0), Double.parseDouble(SafeEncoder.encode((byte[])touple.get(1))));
      }
      return values;
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }
  }
  
  /**
   * TS.INCRBY key [VALUE] [RESET] [TIME_BUCKET]
   * 
   * @param key
   * @param value
   * @param reset
   * @param timeBucket
   * @return
   */
  public boolean incrBy(String key, double value, boolean reset, long timeBucket) {
    try (Jedis conn = getConnection()) {
      return (reset ? 
          sendCommand(conn, Command.INCRBY, SafeEncoder.encode(key), 
              Protocol.toByteArray(value), SafeEncoder.encode("RESET"),  Protocol.toByteArray(timeBucket)) 
          : sendCommand(conn, Command.INCRBY, SafeEncoder.encode(key),  
              Protocol.toByteArray(value), Protocol.toByteArray(reset),  Protocol.toByteArray(timeBucket)))
          .getStatusCodeReply().equals("OK");
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }
  }
  
  /**
   * TS.DECRBY key [VALUE] [RESET] [TIME_BUCKET]
   * 
   * @param key
   * @param value
   * @param reset
   * @param timeBucket
   * @return
   */
  public boolean decrBy(String key, double value, boolean reset, long timeBucket) {
    try (Jedis conn = getConnection()) {
      return (reset ? 
          sendCommand(conn, Command.DECRBY, SafeEncoder.encode(key), 
              Protocol.toByteArray(value), SafeEncoder.encode("RESET"),  Protocol.toByteArray(timeBucket)) 
          : sendCommand(conn, Command.DECRBY, SafeEncoder.encode(key),  
              Protocol.toByteArray(value), Protocol.toByteArray(reset),  Protocol.toByteArray(timeBucket)))
          .getStatusCodeReply().equals("OK");
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }
  }
  
  /**
   * TS.INFO key
   * 
   * @param key
   * @return
   */
  public Map<String, Long> info(String key) {
    return new HashMap<>();
  }
  
  private Jedis getConnection() {
    return pool.getResource();
  }

  private BinaryClient sendCommand(Jedis conn, Command command, byte[]... args) {
    BinaryClient client = conn.getClient();
    client.sendCommand(command, args);
    return client;
}

  
}
