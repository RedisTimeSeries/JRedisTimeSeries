package com.redislabs.redistimeseries;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisTimeSeriesTest {

  private final JedisPool pool = new JedisPool();
  private final RedisTimeSeries client = new RedisTimeSeries(pool); 
  
  @Before
  public void testClient() {
    try (Jedis conn = pool.getResource()) {
      conn.flushAll();
    }      
  }
  
  @Test
  public void testCreate() {
    Assert.assertTrue(client.create("series1", 10/*retentionSecs*/, 10/*maxSamplesPerChunk*/));
    try (Jedis conn = pool.getResource()) {
      Assert.assertEquals("TSDB-TYPE", conn.type("series1"));
    }          
    
    try {
      Assert.assertTrue(client.create("series1", 10/*retentionSecs*/, 10/*maxSamplesPerChunk*/));
      Assert.fail();
    } catch(RedisTimeSeriesException e) {
    }
  }

  @Test
  public void testCreateRule() {
    Assert.assertTrue(client.create("source", 10/*retentionSecs*/, 10/*maxSamplesPerChunk*/));
    Assert.assertTrue(client.create("dest", 10/*retentionSecs*/, 10/*maxSamplesPerChunk*/));
    
    Assert.assertTrue(client.createRule("source", Aggregation.AVG, 100, "dest"));
    
    try {
      Assert.assertFalse(client.createRule("source", Aggregation.COUNT, 100, "dest"));
      Assert.fail();
    } catch(RedisTimeSeriesException e) {
      // Error on creating same rule twice
    }
    
    Assert.assertTrue(client.deleteRule("source", "dest"));
    
    Assert.assertTrue(client.createRule("source", Aggregation.COUNT, 100, "dest"));
  }

}
