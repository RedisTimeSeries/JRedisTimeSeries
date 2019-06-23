package com.redislabs.redistimeseries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.redislabs.redistimeseries.information.Info;
import com.redislabs.redistimeseries.information.Rule;

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
    try (Jedis conn = getConnection()) {      
      return sendCommand(conn, Command.CREATE, SafeEncoder.encode(key)).getStatusCodeReply().equals("OK");
    }
  }


  /**
   * TS.CREATE key [RETENTION retentionTime]
   * 
   * @param key
   * @param retentionTime
   * @return
   */
  public boolean create(String key, long retentionTime){   
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.CREATE, SafeEncoder.encode(key), Keyword.RETENTION.getRaw(), Protocol.toByteArray(retentionTime))
          .getStatusCodeReply().equals("OK");
    }
  }

  /**
   * TS.CREATE key [LABELS field value..]
   * 
   * @param key
   * @param maxSamplesPerChunk
   * @return
   */
  public boolean create(String key, Map<String, String> labels){   
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[1 + (labels==null ? 0 : 2*labels.size()+1)][];
      int i=0;

      args[i++] = SafeEncoder.encode(key);
      if(labels != null) {
        args[i++] = Keyword.LABELS.getRaw();
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.getKey());  
          args[i++] = SafeEncoder.encode(e.getValue());
        }
      }

      return sendCommand(conn, Command.CREATE, args).getStatusCodeReply().equals("OK");
    }
  }

  
  /**
   * TS.CREATE key [RETENTION retentionTime] [LABELS field value..]
   * 
   * @param key
   * @param retentionTime
   * @param maxSamplesPerChunk
   * @return
   */
  public boolean create(String key, long retentionTime, Map<String, String> labels){   
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[3 + (labels==null ? 0 : 2*labels.size()+1)][];
      int i=0;
      
      args[i++] = SafeEncoder.encode(key);
      args[i++] = Keyword.RETENTION.getRaw();
      args[i++] = Protocol.toByteArray(retentionTime);
      
      if(labels != null) {
        args[i++] = Keyword.LABELS.getRaw();
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.getKey());  
          args[i++] = SafeEncoder.encode(e.getValue());
        }
      }

      return sendCommand(conn, Command.CREATE, args).getStatusCodeReply().equals("OK");
    }
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
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[3 + (labels==null ? 0 : 2*labels.size()+1)][];
      int i=0;

      args[i++] = SafeEncoder.encode(sourceKey);
      args[i++] = timestamp>0 ? Protocol.toByteArray(timestamp) : STAR;
      args[i++] = Protocol.toByteArray(value);

      if(labels != null) {
        args[i++] = Keyword.LABELS.getRaw();
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.getKey());  
          args[i++] = SafeEncoder.encode(e.getValue());
        }
      }

      return sendCommand(conn, Command.ADD, args).getIntegerReply();
    }
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
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[5 + (labels==null ? 0 : 2*labels.size()+1)][];
      int i=0;

      args[i++] = SafeEncoder.encode(sourceKey);
      args[i++] = timestamp>0 ? Protocol.toByteArray(timestamp) : STAR;
      args[i++] = Protocol.toByteArray(value);
      args[i++] = Keyword.RETENTION.getRaw();
      args[i++] = Protocol.toByteArray(retentionTime);

      if(labels != null) {
        args[i++] = Keyword.LABELS.getRaw();
        for(Entry<String, String> e : labels.entrySet()) {
          args[i++] = SafeEncoder.encode(e.getKey());  
          args[i++] = SafeEncoder.encode(e.getValue());
        }
      }

      return sendCommand(conn, Command.ADD, args).getIntegerReply();
    }
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
   * TS.RANGE key fromTimestamp toTimestamp
   * 
   * @param key
   * @param from
   * @param to
   * @return
   */
  public Value[] range(String key, long from, long to) {
    try (Jedis conn = getConnection()) {
      List<Object> range = sendCommand(conn, Command.RANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), Protocol.toByteArray(to))
          .getObjectMultiBulkReply();

      Value[] values = new Value[range.size()];

      for(int i=0; i<values.length ; ++i) {
        List<Object> touple = (List<Object>)range.get(i);
        values[i] = new Value((Long)touple.get(0), Double.parseDouble(SafeEncoder.encode((byte[])touple.get(1))));
      }
      return values;
    }
  }

  /**
   * TS.RANGE key fromTimestamp toTimestamp [AGGREGATION aggregationType
   * retentionTime]
   * 
   * @param key
   * @param from
   * @param to
   * @param aggregation
   * @param retentionTime
   * @return
   */
  public Value[] range(String key, long from, long to, Aggregation aggregation, long retentionTime) {
    try (Jedis conn = getConnection()) {
      List<Object> range = sendCommand(conn, Command.RANGE, SafeEncoder.encode(key), Protocol.toByteArray(from), 
          Protocol.toByteArray(to), Keyword.AGGREGATION.getRaw(), aggregation.getRaw(), Protocol.toByteArray(retentionTime))
          .getObjectMultiBulkReply();

      Value[] values = new Value[range.size()];

      for(int i=0; i<values.length ; ++i) {
        List<Object> touple = (List<Object>)range.get(i);
        values[i] = new Value((Long)touple.get(0), Double.parseDouble(SafeEncoder.encode((byte[])touple.get(1))));
      }
      return values;
    }
  }


  /**
   * TS.RANGEBYLABELS key (labels) fromTimestamp toTimestamp [aggregationType] [retentionTime]
   * 
   * @param key
   * @param from
   * @param to
   * @param aggregation
   * @param retentionTime
   * @param filters
   * @return
   */
  public Range[] mrange(long from, long to, Aggregation aggregation, long retentionTime, String... filters) {
    try (Jedis conn = getConnection()) {

      byte[][] args = new byte[6 + (filters==null ? 0 : filters.length)][];
      int i=0;

      args[i++] = Protocol.toByteArray(from);
      args[i++] = Protocol.toByteArray(to);
      args[i++] = Keyword.AGGREGATION.getRaw();
      args[i++] = aggregation.getRaw();
      args[i++] = Protocol.toByteArray(retentionTime);

      args[i++] = Keyword.FILTER.getRaw();
      if(filters != null) {
        for(String label : filters) {
          args[i++] = SafeEncoder.encode(label);  
        }
      }

      List<?> result = sendCommand(conn, Command.MRANGE, args).getObjectMultiBulkReply();
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
    }
  }

  /**
   * TS.INCRBY key value [RESET time-bucket]
   * 
   * @param key
   * @param value
   * @param timeBucket
   * @return
   */
  public boolean incrBy(String key, int value, long timeBucket) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.INCRBY, SafeEncoder.encode(key), Protocol.toByteArray(value), 
          Keyword.RESET.getRaw(), Protocol.toByteArray(timeBucket)) 
          .getStatusCodeReply().equals("OK");
    }
  }

  /**
   * TS.DECRBY key [VALUE] [RESET] [TIME_BUCKET]
   * 
   * @param key
   * @param value
   * @param timeBucket
   * @return
   */
  public boolean decrBy(String key, int value, long timeBucket) {
    try (Jedis conn = getConnection()) {
      return sendCommand(conn, Command.DECRBY, SafeEncoder.encode(key), Protocol.toByteArray(value), 
          Keyword.RESET.getRaw(),  Protocol.toByteArray(timeBucket)) 
          .getStatusCodeReply().equals("OK");
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
