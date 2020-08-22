package com.redislabs.redistimeseries;

import java.util.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.redislabs.redistimeseries.information.Info;
import com.redislabs.redistimeseries.information.Rule;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisTimeSeriesTest {

  private final JedisPool pool = new JedisPool();
  private final RedisTimeSeries client = new RedisTimeSeries();

  @Before
  public void testClient() {
    try (Jedis conn = pool.getResource()) {
      conn.flushAll();
    }
  }

  @Test
  public void testCreate() {
    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");

    Assert.assertTrue(client.create("series1", 10/*retentionTime*/, labels));
    try (Jedis conn = pool.getResource()) {
      Assert.assertEquals("TSDB-TYPE", conn.type("series1"));
    }

    Assert.assertTrue(client.create("series2", labels));
    try (Jedis conn = pool.getResource()) {
      Assert.assertEquals("TSDB-TYPE", conn.type("series2"));
    }

    Assert.assertTrue(client.create("series3", 10/*retentionTime*/));
    try (Jedis conn = pool.getResource()) {
      Assert.assertEquals("TSDB-TYPE", conn.type("series3"));
    }

    Assert.assertTrue(client.create("series4"));
    try (Jedis conn = pool.getResource()) {
      Assert.assertEquals("TSDB-TYPE", conn.type("series4"));
    }

    Assert.assertTrue(client.create("series5", 56/*retentionTime*/, true, labels));
    try (Jedis conn = pool.getResource()) {
      Assert.assertEquals("TSDB-TYPE", conn.type("series5"));
    }

    try {
      Assert.assertTrue(client.create("series1", 10/*retentionTime*/, labels));
      Assert.fail();
    } catch(JedisDataException e) {
    }

    try {
      Assert.assertTrue(client.create("series1", labels));
      Assert.fail();
    } catch(JedisDataException e) {
    }

    try {
      Assert.assertTrue(client.create("series1", 10));
      Assert.fail();
    } catch(JedisDataException e) {
    }

    try {
      Assert.assertTrue(client.create("series1"));
      Assert.fail();
    } catch(JedisDataException e) {
    }

  }

  @Test
  public void testRule() {
    Assert.assertTrue(client.create("source"));
    Assert.assertTrue(client.create("dest", 10/*retentionTime*/));

    Assert.assertTrue(client.createRule("source", Aggregation.AVG, 100, "dest"));

    try {
      Assert.assertFalse(client.createRule("source", Aggregation.COUNT, 100, "dest"));
      Assert.fail();
    } catch(JedisDataException e) {
      // Error on creating same rule twice
    }

    Assert.assertTrue(client.deleteRule("source", "dest"));
    Assert.assertTrue(client.createRule("source", Aggregation.COUNT, 100, "dest"));

    try {
      Assert.assertTrue(client.deleteRule("source", "dest1"));
      Assert.fail();
    } catch(JedisDataException e) {
      // Error on creating same rule twice
    }
  }

  @Test
  public void testAdd() {
    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");
    Assert.assertTrue(client.create("seriesAdd", 10000L/*retentionTime*/, labels));

    Assert.assertEquals(1000L, client.add("seriesAdd", 1000L, 1.1, 10000, null));
    Assert.assertEquals(2000L, client.add("seriesAdd", 2000L, 0.9, null));
    Assert.assertEquals(3200L, client.add("seriesAdd", 3200L, 1.1, 10000));
    Assert.assertEquals(4500L, client.add("seriesAdd", 4500L, -1.1));

    Value[] rawValues = new  Value[]{new Value(1000L,1.1), new Value(2000L,0.9), new Value(3200L,1.1), new Value(4500L,-1.1) };
    Value[] values = client.range("seriesAdd", 800L, 3000L);
    Assert.assertEquals(2, values.length);
    Assert.assertArrayEquals(values, Arrays.copyOfRange(rawValues, 0, 2));
    values = client.range("seriesAdd", 800L, 5000L);
    Assert.assertEquals(4, values.length);
    Assert.assertArrayEquals(rawValues, values);

    Value[] expectedCountValues = new  Value[]{new Value(2000L,1), new Value(3200L,1), new Value(4500L,1) };
    values = client.range("seriesAdd", 1200L, 4600L, Aggregation.COUNT, 1);
    Assert.assertEquals(3, values.length);
    Assert.assertArrayEquals(expectedCountValues, values);

    Value[] expectedAvgValues = new  Value[]{new Value(0L,1.1), new Value(2000L,1), new Value(4000L,-1.1) };
    values = client.range("seriesAdd", 500L, 4600L, Aggregation.AVG, 2000L);
    Assert.assertEquals(3, values.length);
    Assert.assertArrayEquals(expectedAvgValues, values);

    // ensure zero-based index
    Value[] valuesZeroBased = client.range("seriesAdd", 0L, 4600L, Aggregation.AVG, 2000L);
    Assert.assertEquals(3, valuesZeroBased.length);
    Assert.assertArrayEquals(values,valuesZeroBased);

    Value[] expectedOverallSumValues = new  Value[]{new Value(0L,2.0)};
    values = client.range("seriesAdd", 0L, 5000L, Aggregation.SUM, 5000L);
    Assert.assertEquals(1, values.length);
    Assert.assertArrayEquals(expectedOverallSumValues, values);

    Value[] expectedOverallMinValues = new  Value[]{new Value(0L,-1.1)};
    values = client.range("seriesAdd", 0L, 5000L, Aggregation.MIN, 5000L);
    Assert.assertEquals(1, values.length);
    Assert.assertArrayEquals(expectedOverallMinValues, values);

    Value[] expectedOverallMaxValues = new  Value[]{new Value(0L,1.1)};
    values = client.range("seriesAdd", 0L, 5000L, Aggregation.MAX, 5000L);
    Assert.assertEquals(1, values.length);
    Assert.assertArrayEquals(expectedOverallMaxValues, values);

    try {
      client.mrange(500L, 4600L, Aggregation.COUNT, 1);
      Assert.fail();
    }catch(JedisDataException e) {}

    try {
      client.mrange(500L, 4600L, Aggregation.COUNT, 1, (String[])null);
      Assert.fail();
    }catch(JedisDataException e) {}

    Range[] ranges = client.mrange(500L, 4600L, Aggregation.COUNT, 1, "l1=v1");
    Assert.assertEquals(1, ranges.length);

    Range range = ranges[0];
    Assert.assertEquals("seriesAdd", range.getKey());
    Assert.assertEquals(new HashMap<>(), range.getLabels());

    Value[] rangeValues = range.getValues();
    Assert.assertEquals(4, rangeValues.length);
    Assert.assertEquals( new Value(1000, 1), rangeValues[0]);
    Assert.assertNotEquals( new Value(1000, 1.1), rangeValues[0]);
    Assert.assertEquals( 2000L, rangeValues[1].getTime());
    Assert.assertEquals( "(2000:1.0)", rangeValues[1].toString());
    Assert.assertEquals( 1072695248, rangeValues[1].hashCode());
    Assert.assertNotEquals("(2000:1.0)", rangeValues[1]); // verify wrong type

    // Add with labels
    Map<String, String> labels2 = new HashMap<>();
    labels2.put("l3", "v3");
    labels2.put("l4", "v4");
    Assert.assertEquals(1000L, client.add("seriesAdd2", 1000L, 1.1, 10000, labels2));
    Range[] ranges2 = client.mrange(500L, 4600L, Aggregation.COUNT, 1, true, "l4=v4");
    Assert.assertEquals(1, ranges2.length);
    Assert.assertEquals(labels2, ranges2[0].getLabels());

    Map<String, String> labels3 = new HashMap<>();
    labels3.put("l3", "v33");
    labels3.put("l4", "v4");
    Assert.assertEquals(1000L, client.add("seriesAdd3", 1000L, 1.1, labels3));
    Assert.assertEquals(2000L, client.add("seriesAdd3", 2000L, 1.1, labels3));
    Assert.assertEquals(3000L, client.add("seriesAdd3", 3000L, 1.1, labels3));
    Range[] ranges3 = client.mrange(500L, 4600L, Aggregation.AVG, 1, true, 2, "l4=v4");
    Assert.assertEquals(2, ranges3.length);
    Assert.assertEquals(1, ranges3[0].getValues().length);
    Assert.assertEquals(labels2, ranges3[0].getLabels());
    Assert.assertEquals(2, ranges3[1].getValues().length);
    Assert.assertEquals(labels3, ranges3[1].getLabels());

    // Back filling
    Assert.assertEquals(800L, client.add("seriesAdd", 800L, 1.1));
    Assert.assertEquals(700L, client.add("seriesAdd", 700L, 1.1, 10000));
    Assert.assertEquals(600L, client.add("seriesAdd", 600L, 1.1, 10000, null));

    // Range on none existing key
    try {
      client.range("seriesAdd1", 500L, 4000L, Aggregation.COUNT, 1);
      Assert.fail();
    } catch(JedisDataException e) {
    }
  }

  @Test
  public void testValue() {
    Value v = new Value(1234, 234.89634);
    Assert.assertEquals(1234, v.getTime());
    Assert.assertEquals(234.89634, v.getValue(), 0);

    Assert.assertTrue(v.equals(new Value(1234, 234.89634)));
    Assert.assertFalse(v.equals(new Value(1334, 234.89634)));
    Assert.assertFalse(v.equals(new Value(1234, 234.8934)));
    Assert.assertFalse(v.equals(1234));

    Assert.assertEquals("(1234:234.89634)", v.toString());
    Assert.assertEquals(-1856758940, v.hashCode());
  }

  @Test
  public void testAddStar() throws InterruptedException {
    Map<String, String> labels = new HashMap<>();
    labels.put("l11", "v11");
    labels.put("l22", "v22");
    Assert.assertTrue(client.create("seriesAdd2", 10000L/*retentionTime*/, labels));

    long startTime = System.currentTimeMillis();
    Thread.sleep(1);
    long add1 = client.add("seriesAdd2", 0, 1.1, 10000, null);
    Assert.assertTrue(add1>startTime);
    Thread.sleep(1);
    long add2 = client.add("seriesAdd2", 0, 3.2, null);
    Assert.assertTrue(add2>add1);
    Thread.sleep(1);
    long add3 = client.add("seriesAdd2", 0, 3.2, 10000);
    Assert.assertTrue(add3>add2);
    Thread.sleep(1);
    long add4 = client.add("seriesAdd2", -1.2);
    Assert.assertTrue(add4>add3);
    Thread.sleep(1);
    long endTime = System.currentTimeMillis();
    Assert.assertTrue(endTime>add4);

    Value[] values = client.range("seriesAdd2", startTime, add3);
    Assert.assertEquals(3, values.length);
  }

  @Test
  public void testMadd() {
    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");
    Assert.assertTrue(client.create("seriesAdd1", 10000L/*retentionTime*/, labels));
    Assert.assertTrue(client.create("seriesAdd2", 10000L/*retentionTime*/, labels));

    long now = System.currentTimeMillis();
    List<Object> result = client.madd(
        new Measurement("seriesAdd1", 0L, 1.1), // System time
        new Measurement("seriesAdd2", 2000L, 3.2),
        new Measurement("seriesAdd1", 1500L, 2.67), // Should return an error
        new Measurement("seriesAdd2", 3200L, 54.2),
        new Measurement("seriesAdd2", 4300L, 21.2));

    Assert.assertTrue(now <= (Long)result.get(0) && now+5 > (Long)result.get(0));
    Assert.assertEquals(2000L, result.get(1));
    Assert.assertTrue( result.get(2) instanceof JedisDataException);
    Assert.assertEquals(3200L, result.get(3));
    Assert.assertEquals(4300L, result.get(4));

    Value[] values1 = client.range("seriesAdd1", 0, Long.MAX_VALUE);
    Assert.assertEquals(1, values1.length);
    Assert.assertEquals(1.1, values1[0].getValue(), 0.001);

    Value[] values2 = client.range("seriesAdd2", 0, Long.MAX_VALUE, 2);
    Assert.assertEquals(2, values2.length);
    Assert.assertEquals(3.2, values2[0].getValue(), 0.001);
    Assert.assertEquals(54.2, values2[1].getValue(), 0.001);
  }

  @Test
  public void testIncDec() throws InterruptedException {
    Assert.assertTrue(client.create("seriesIncDec", 100*1000/*100sec retentionTime*/));
    long startTime = System.currentTimeMillis();
    Assert.assertEquals(startTime, client.add("seriesIncDec", -1, 1, 10000, null), 0);

    Thread.sleep(1);
    startTime = System.currentTimeMillis();
    Assert.assertEquals(startTime, client.incrBy("seriesIncDec", 3, startTime), 0);

    Thread.sleep(1);
    startTime = System.currentTimeMillis();
    Assert.assertEquals(startTime, client.decrBy("seriesIncDec", 2, startTime), 0);

    Value[] values = client.range("seriesIncDec", 1L, Long.MAX_VALUE);
    Assert.assertEquals(3, values.length);
    Assert.assertEquals(2, values[2].getValue(), 0);

    Thread.sleep(1);
    startTime = System.currentTimeMillis();
    Assert.assertEquals(startTime, client.incrBy("seriesIncDec", 3), 0);

    Thread.sleep(1);
    startTime = System.currentTimeMillis();
    Assert.assertEquals(startTime, client.decrBy("seriesIncDec", 2), 0);

    Assert.assertEquals(startTime, client.decrBy("seriesIncDec", 2, startTime), 0);

    values = client.range("seriesIncDec", 1L, Long.MAX_VALUE);
    Assert.assertEquals(5, values.length);
    Assert.assertEquals(1, values[4].getValue(), 0);

    try {
      client.incrBy("seriesIncDec", 3, startTime-1);
      Assert.fail();
    } catch(JedisDataException e) {
      // Error on incrby in the past
    }
  }

  @Test
  public void testGet(){

    // Test for empty result none existing series
    try {
      client.get("seriesGet");
      Assert.fail();
    }catch(JedisDataException e) {}

    Assert.assertTrue(client.create("seriesGet", 100*1000/*100sec retentionTime*/));

    // Test for empty result
    Assert.assertNull(client.get("seriesGet"));

    // Test returned last Value
    client.add("seriesGet", 2558, 8.7);
    Assert.assertEquals(new Value(2558, 8.7), client.get("seriesGet"));

    client.add("seriesGet", 3458, 1.117);
    Assert.assertEquals(new Value(3458, 1.117), client.get("seriesGet"));
  }

  @Test
  public void testMGet(){
    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");
    Assert.assertTrue(client.create("seriesMGet1", 100*1000/*100sec retentionTime*/, labels));
    Assert.assertTrue(client.create("seriesMGet2", 100*1000/*100sec retentionTime*/, labels));

    // Test for empty result
    Range[] ranges1 = client.mget(false, "l1=v2");
    Assert.assertEquals(0, ranges1.length);

    // Test for empty ranges
    Range[] ranges2 = client.mget(true, "l1=v1");
    Assert.assertEquals(2, ranges2.length);
    Assert.assertEquals(labels, ranges2[0].getLabels());
    Assert.assertEquals(labels, ranges2[1].getLabels());
    Assert.assertEquals(0, ranges2[0].getValues().length);

    // Test for returned result on MGet
    client.add("seriesMGet1", 1500, 1.3);
    Range[] ranges3 = client.mget(false, "l1=v1");
    Assert.assertEquals(2, ranges3.length);
    Assert.assertEquals(new HashMap<String, String>(), ranges3[0].getLabels());
    Assert.assertEquals(new HashMap<String, String>(), ranges3[1].getLabels());
    Assert.assertEquals(1, ranges3[0].getValues().length);
    Assert.assertEquals(0, ranges3[1].getValues().length);
    Assert.assertEquals(new Value(1500, 1.3), ranges3[0].getValues()[0]);
  }

  @Test
  public void testAlter(){

    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");
    Assert.assertTrue(client.create("seriesAlter", 57*1000/*57sec retentionTime*/, labels));
    Assert.assertArrayEquals( new String[0], client.queryIndex("l2=v22"));

    // Test alter labels
    labels.remove("l1");
    labels.put("l2", "v22");
    labels.put("l3", "v33");
    Assert.assertTrue(client.alter("seriesAlter", labels));
    Assert.assertArrayEquals( new String[]{"seriesAlter"}, client.queryIndex("l2=v22", "l3=v33"));
    Assert.assertArrayEquals( new String[0], client.queryIndex("l1=v1"));

    // Test alter labels and retention time
    labels.put("l1", "v11");
    labels.remove("l2");
    Assert.assertTrue(client.alter("seriesAlter", 324/*324ms retentionTime*/, labels));
    Info info = client.info("seriesAlter");
    Assert.assertEquals( (Long)324L, info.getProperty("retentionTime"));
    Assert.assertEquals( "v11", info.getLabel("l1"));
    Assert.assertEquals( null, info.getLabel("l2"));
    Assert.assertEquals( "v33", info.getLabel("l3"));
  }

  @Test
  public void testQueryIndex(){

    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");
    Assert.assertTrue(client.create("seriesQueryIndex1", 100*1000/*100sec retentionTime*/, labels));

    labels.put("l2", "v22");
    labels.put("l3", "v33");
    Assert.assertTrue(client.create("seriesQueryIndex2", 100*1000/*100sec retentionTime*/, labels));

    Assert.assertArrayEquals( new String[0], client.queryIndex("l1=v2"));
    Assert.assertArrayEquals( new String[]{"seriesQueryIndex1", "seriesQueryIndex2"}, client.queryIndex("l1=v1"));
    Assert.assertArrayEquals( new String[]{"seriesQueryIndex2"}, client.queryIndex("l2=v22"));
  }


  @Test
  public void testInfo() {
    Map<String, String> labels = new HashMap<>();
    labels.put("l1", "v1");
    labels.put("l2", "v2");
    Assert.assertTrue(client.create("source", 10000L/*retentionTime*/, labels));
    Assert.assertTrue(client.create("dest", 20000L/*retentionTime*/));
    Assert.assertTrue(client.createRule("source", Aggregation.AVG, 100, "dest"));

    Info info = client.info("source");
    Assert.assertEquals( (Long)10000L, info.getProperty("retentionTime"));
    Assert.assertEquals( "v1", info.getLabel("l1"));
    Assert.assertEquals( "v2", info.getLabel("l2"));
    Assert.assertEquals( null, info.getLabel("l3"));

    Rule rule = info.getRule("dest");
    Assert.assertEquals( "dest", rule.getTarget());
    Assert.assertEquals( 100L, rule.getValue());
    Assert.assertEquals( Aggregation.AVG, rule.getAggregation());
    try {
      client.info("seriesInfo1");
      Assert.fail();
    } catch(JedisDataException e) {
      // Error on info on none existing series
    }
  }
}
