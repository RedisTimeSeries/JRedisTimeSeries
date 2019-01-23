package com.redislabs.redistimeseries;

import redis.clients.jedis.exceptions.JedisDataException;

public class RedisTimeSeriesException extends RuntimeException{
  
  public RedisTimeSeriesException(JedisDataException cause) {
    super(cause);
  }

}
