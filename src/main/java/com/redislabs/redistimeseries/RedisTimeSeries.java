package com.redislabs.redistimeseries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.redislabs.redistimeseries.information.Info;
import com.redislabs.redistimeseries.information.Rule;

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
  public boolean create(String key, long retentionSecs, long maxSamplesPerChunk, Map<String, String> labels){   
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[3 + (labels==null ? 0 : labels.size())][];
      
      int i=0;
      args[i++] = SafeEncoder.encode(key);
      args[i++] = Protocol.toByteArray(retentionSecs);
      args[i++] = Protocol.toByteArray(maxSamplesPerChunk);
      
      if(labels != null) {
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.toString());  
        }
      }
      
      return sendCommand(conn, Command.CREATE, args).getStatusCodeReply().equals("OK");
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
   * TS.ADD key [labels] timestamp value
   * 
   * @param sourceKey
   * @param timestamp
   * @param value
   * @param labels
   * @return
   */
  public boolean add(String sourceKey, long timestamp, double value, Map<String, String> labels) {
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[3 + (labels==null ? 0 : labels.size())][];
      
      int i=0;
      args[i++] = SafeEncoder.encode(sourceKey);
      
      if(labels != null) {
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.toString());  
        }
      }
      
      args[i++] = timestamp>0? Protocol.toByteArray(timestamp) :  SafeEncoder.encode("*");
      args[i++] = Protocol.toByteArray(value);
      
      return sendCommand(conn, Command.ADD, args).getStatusCodeReply().equals("OK");
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
    }
  }
  
  /**
   * TS.RANGE key fromTimestamp toTimestamp [aggregationType] [bucketSizeSeconds]
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
   * TS.RANGEBYLABELS key (labels) fromTimestamp toTimestamp [aggregationType] [bucketSizeSeconds]
   * 
   * @param key
   * @param from
   * @param to
   * @param aggregation
   * @param bucketSizeSeconds
   * @return
   */
  public Range[] range(long from, long to, Aggregation aggregation, long bucketSizeSeconds, Map<String,String> labels) {
    try (Jedis conn = getConnection()) {
      
      byte[][] args = new byte[4 + (labels==null ? 0 : labels.size())][];
      int i=0;
      
      if(labels != null) {
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.toString());  
        }
      }
     
      args[i++] = Protocol.toByteArray(from);
      args[i++] = Protocol.toByteArray(to);
      args[i++] = aggregation.getRaw();
      args[i++] = Protocol.toByteArray(bucketSizeSeconds);
      
      List<?> result = sendCommand(conn, Command.RANGEBYLABELS, args).getObjectMultiBulkReply();
      Range[] ranges = new Range[result.size()];
      for(int j=0; j<ranges.length; ++j) {
        List<?> series = (List<?>)result.get(j);
        
        String resKey = SafeEncoder.encode((byte[])series.get(0));
        
        
        List<?> resLables = (List<?>)series.get(1);
        Map<String, String> rangeLabels = new HashMap<>();
        for(int l=0; l<resLables.size(); ++l) {
          List<byte[]> label = (List<byte[]>)resLables.get(l);
          rangeLabels.put( SafeEncoder.encode(label.get(0)), SafeEncoder.encode(label.get(1)));
        }   
        
        List<?> resRange = (List<?>)series.get(2);
        Value[] values = new Value[resRange.size()];
        for(int r=0; r<values.length ; ++r) {
          List<?> touple = (List<?>)resRange.get(r);
          values[r] = new Value((Long)touple.get(0), Double.parseDouble(SafeEncoder.encode((byte[])touple.get(1))));
        }   
        
        ranges[j] = new Range(resKey, rangeLabels, values);
      }
      return ranges;
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
  public boolean incrBy(String key, int value, boolean reset, long timeBucket) {
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
  public boolean decrBy(String key, int value, boolean reset, long timeBucket) {
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
  public Info info(String key) {
    try (Jedis conn = getConnection()) {
      List<Object> resp = sendCommand(conn, Command.INFO, SafeEncoder.encode(key))
          .getObjectMultiBulkReply();

      Map<String, Long> properties = new HashMap<>();
      Map<String, String> labels = new HashMap<>();
      Map<String, Rule> rules = new HashMap<>();
      for(int i=0; i<resp.size() ; i+=2) {
        String prop = SafeEncoder.encode((byte[])resp.get(i));
        Object value = resp.get(i+1);
        if(value instanceof Long) {
          properties.put(prop, (Long)value);
        } else {
          if(prop.equals("labels")) {
            List<Object> labelsList = (List<Object>)value;
            for(int j=0; j<labelsList.size() ; j+=2) {
              labels.put( SafeEncoder.encode((byte[])labelsList.get(j)), SafeEncoder.encode((byte[])labelsList.get(j+1)));
            }
          }
//       TODO   else if(prop.equals("rules") ) {
//            List<Object> rulesList = (List<Object>)value;
//            for(int j=0; j<labelsList.size() ; j+=2) {
//              labels.put( SafeEncoder.encode((byte[])labelsList.get(j)), SafeEncoder.encode((byte[])labelsList.get(j+1)));
//            }
//          } 
        }
      }
      return new Info(properties, labels, rules);
    } catch(JedisDataException ex ) {
      throw new RedisTimeSeriesException(ex);
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

  
}
