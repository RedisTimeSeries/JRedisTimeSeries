[![license](https://img.shields.io/github/license/RedisTimeSeries/JRedisTimeSeries.svg)](https://github.com/RedisTimeSeries/JRedisTimeSeries)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/JRedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/JRedisTimeSeries/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/JRedisTimeSeries.svg)](https://github.com/RedisTimeSeries/JRedisTimeSeries/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredistimeseries/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredistimeseries)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jredistimeseries.svg)](https://www.javadoc.io/doc/com.redislabs/jredistimeseries)
[![Codecov](https://codecov.io/gh/RedisTimeSeries/JRedisTimeSeries/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/JRedisTimeSeries)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/RedisTimeSeries/JRedisTimeSeries.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisTimeSeries/JRedisTimeSeries/context:java)

# JRedisTimeSeries
Java Client for RedisTimeSeries

### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredistimeseries</artifactId>
      <version>1.2.1</version>
    </dependency>
  </dependencies>
```

### Snapshots

```xml
  <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
  </repositories>
```

and

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredistimeseries</artifactId>
      <version>1.3.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```


# Example: Using the Java Client

```java
   RedisTimeSeries rts = new RedisTimeSeries("localhost", 6379);
   
   Map<String, String> labels = new HashMap<>();
   labels.put("country", "US");
   labels.put("cores", "8"); 
   rts.create("cpu1", 60*10 /*10min*/, 100, labels);
   
   rts.create("cpu1-avg", 60*10 /*10min*/, 100, null);
   rts.createRule("cpu1", Aggregation.AVG, 60 /*1min*/, "cpu1-avg");
   
   rts.add("cpu1", System.currentTimeMillis()/1000 /* time sec */, 80);
   
   // Get all the timeseries in US in the last 10min average per min  
   rts.mrange(System.currentTimeMillis()/1000 - 10*60, System.currentTimeMillis()/1000, Aggregation.AVG, 60, "country=US")
```


## Supported RedisTimeSeries Commands

| Command | Supported since  |
| :---          |  ----: |
| [TS.CREATE](https://oss.redislabs.com/redistimeseries/commands/#tscreate) |   [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)    |
| [TS.ALTER](https://oss.redislabs.com/redistimeseries/commands/#tsalter) |   N/A          |
| [TS.ADD](https://oss.redislabs.com/redistimeseries/commands/#tsadd) |   [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)    |
| [TS.MADD](https://oss.redislabs.com/redistimeseries/commands/#tsmadd) |    [0.9.0](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.9.0/index.html) |
| [TS.INCRBY/TS.DECRBY](https://oss.redislabs.com/redistimeseries/commands/#tsincrbytsdecrby) |    [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)         |
| [TS.CREATERULE](https://oss.redislabs.com/redistimeseries/commands/#tscreaterule) |   [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)       |
| [TS.DELETERULE](https://oss.redislabs.com/redistimeseries/commands/#tsdeleterule) |   [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)       |
| [TS.RANGE](https://oss.redislabs.com/redistimeseries/commands/#tsrange) |   [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)        |
| [TS.MRANGE](https://oss.redislabs.com/redistimeseries/commands/#tsmrange) |   [0.2.0](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.2.0/index.html)         |
| [TS.GET](https://oss.redislabs.com/redistimeseries/commands/#tsget) |   [1.3.0](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/1.3.0/index.html)          |
| [TS.MGET](https://oss.redislabs.com/redistimeseries/commands/#tsmget) |  [1.3.0](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/1.3.0/index.html)          |
| [TS.INFO](https://oss.redislabs.com/redistimeseries/commands/#tsinfo) |   [0.0.1](https://www.javadoc.io/doc/com.redislabs/jredistimeseries/0.0.1/index.html)          |
| [TS.QUERYINDEX](https://oss.redislabs.com/redistimeseries/commands/#tsqueryindex) |    N/A |
