[![license](https://img.shields.io/github/license/RedisTimeSeries/JRedisTimeSeries.svg)](https://github.com/RedisTimeSeries/JRedisTimeSeries)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/JRedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/JRedisTimeSeries/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/JRedisTimeSeries.svg)](https://github.com/RedisTimeSeries/JRedisTimeSeries/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredistimeseries/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredistimeseries)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jredistimeseries.svg)](https://www.javadoc.io/doc/com.redislabs/jredistimeseries)
[![Codecov](https://codecov.io/gh/RedisTimeSeries/JRedisTimeSeries/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/JRedisTimeSeries)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/RedisTimeSeries/JRedisTimeSeries.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisTimeSeries/JRedisTimeSeries/context:java)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisTimeSeries/JRedisTimeSeries/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RedisTimeSeries/JRedisTimeSeries?targetFile=pom.xml)

# JRedisTimeSeries
[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redislabs.com/c/modules/redistimeseries)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)

Java Client for RedisTimeSeries

### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredistimeseries</artifactId>
      <version>1.4.0</version>
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
      <version>1.5.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```


# Example: Using the Java Client

```java
   RedisTimeSeries rts = new RedisTimeSeries("localhost", 6379);
   
   Map<String, String> labels = new HashMap<>();
   labels.put("country", "US");
   labels.put("cores", "8"); 
   rts.create("cpu1", 60*10 /*10min*/, labels);
   
   rts.create("cpu1-avg", 60*10 /*10min*/, null);
   rts.createRule("cpu1", Aggregation.AVG, 60 /*1min*/, "cpu1-avg");
   
   rts.add("cpu1", System.currentTimeMillis()/1000 /* time sec */, 80.0);
   
   // Get all the timeseries in US in the last 10min average per min  
   rts.mrange(System.currentTimeMillis()/1000 - 10*60, System.currentTimeMillis()/1000, Aggregation.AVG, 60, "country=US")
```
