package com.redislabs.tsdb.demo;

import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.redislabs.redistimeseries.Aggregation;
import com.redislabs.redistimeseries.Range;
import com.redislabs.redistimeseries.RedisTimeSeries;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class JRedisTimeSeriesTest {
	private static JedisPool pool;
	private static RedisTimeSeries rts;
	private static JedisPoolConfig config;

	private static int connectTimeout = 30000; // milliseconds
	private static int socketTimeout = 20000; // milliseconds
	private static int iter = 1; // iteration count (for long running test)

	private static String host = "localhost"; // Use HA Redis DB e.g. Redis Enterprise for auto-connect test
	private static int port = 6379; // use same port value i.e. port == sslport for SSL connection
	private static int sslport = 6380; // use same port value i.e. port == sslport for SSL connection
	private static String password;

	public static void main(String[] args) throws Exception {

		rts = new RedisTimeSeries(getPoolInstance());
		
		Map<String, String> labels = new HashMap<>();
		labels.put("country", "US");
		labels.put("cores", "8");

		try {
			rts.create("cpu1", 60*10 /*10min*/, true, labels);
			rts.create("cpu1-avg", 60*10 /*10min*/, true, null);
			rts.createRule("cpu1", Aggregation.AVG, 60 /*1min*/, "cpu1-avg");

			for (int i = 0; i <= iter; i++) { // loop for auto reconnect test
				long rts_add = rts.add("cpu1", System.currentTimeMillis()/1000 /*time sec*/, 80);
				Thread.sleep(1000); // sleeping for localhost or same machine test
				System.out.println("Iteration-" + i + ": " + rts_add);
			} // loop for auto reconnect test

			// Get all the timeseries in US in the last 10min average per minute
			Range[] rts_mranges = rts.mrange(System.currentTimeMillis()/1000 - 10*60, System.currentTimeMillis()/1000, Aggregation.AVG,
					60, "country=US");
			
			Range rts_mrange = rts_mranges[0];
			
			//Info rts_info = rts.info("cpu1");
			System.out.println("Key: " + rts_mrange.getKey());
			System.out.println("Labels: " + rts_mrange.getLables());
			//System.out.println("Labels: {country=" + rts_info.getLabel("country") + 
			//		", cores=" + rts_info.getLabel("cores") + "}");
			System.out.println("Value: " + rts.get(rts_mrange.getKey()));

		} catch (Exception e) {
			Thread.sleep(1000L);
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		System.out.println(getPoolCurrentUsage());
		/// ... when closing application:
		getPoolInstance().close();
	}

	private static JedisPool getPoolInstance() {
		if (pool == null) {
			JedisPoolConfig poolConfig = getPoolConfig();
			boolean useSsl = port == sslport ? true : false;
			int db = 0;
			String clientName = "JRedisTimeSeriesTest";
			SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
			SSLParameters sslParameters = new SSLParameters();
			//HostnameVerifier hostnameVerifier = new SimpleHostNameVerifier(host);
			if (useSsl) {
				pool = new JedisPool(poolConfig, host, port, connectTimeout, socketTimeout, password, db, clientName,
						useSsl, sslSocketFactory, sslParameters, null);
			} else {
				pool = new JedisPool(poolConfig, host, port, connectTimeout, socketTimeout, password, db, clientName);
			}
		}
		return pool;
	}

	private static JedisPoolConfig getPoolConfig() {
		if (config == null) {
			JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

			// Connection testings
			// To be able to run the while idle test Jedis Pool must set the evictor
			// thread (in "general" section). We will also set the pool to be static
			// so no idle connections could get evicted.

			// Send Redis PING on borrow
			// Recommendation (False), reason - additional RTT on the connection exactly
			// when the app needs it, reduces performance.
			jedisPoolConfig.setTestOnBorrow(false);

			// Send Redis PING on create
			// Recommendation (False), reason - password makes it completely
			// redundant as Jedis sends AUTH
			jedisPoolConfig.setTestOnCreate(false);

			// Send Redis PING on return
			// Recommendation (False), reason - the connection will get tested with
			// the Idle test. No real need here. No impact for true as well.
			jedisPoolConfig.setTestOnReturn(false);

			// Send periodic Redis PING for idle pool connections
			// Recommendation (True), reason - test and heal connections while
			// they are idle in the pool.
			jedisPoolConfig.setTestWhileIdle(true);

			// Dynamic pool configuration
			// This is advanced configuration and the suggestion for most use-cases
			// is to leave the pool static
			// If you need your pool to be dynamic make sure you understand the
			// configuration options

			jedisPoolConfig.setMaxIdle(0);
			jedisPoolConfig.setMinIdle(8);
			jedisPoolConfig.setEvictorShutdownTimeoutMillis(-1);
			jedisPoolConfig.setMinEvictableIdleTimeMillis(-1);
			jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(-1);

			// jedisPoolConfig.setJmxEnabled(true);
			// jedisPoolConfig.setJmxNameBase("JRedisTimeSeriesTest");
			// jedisPoolConfig.setJmxNamePrefix("jrtt");

			// Scheduling algorithms (Leave the defaults)

			// Set to true to have LIFO behavior (always returning the most recently
			// used object from the pool). Set to false to have FIFO behavior
			// Recommendation (?) Default value is True and for now is also the
			// recommendation
			jedisPoolConfig.setLifo(true);
			// Returns whether or not the pool serves threads waiting to borrow
			// objects fairly.
			// True means that waiting threads are served as if waiting in a FIFO
			// queue.
			// False ??maybe?? relies on the OS scheduling
			// Recommendation (?) Default value is False and for now is also
			// the recommendation
			jedisPoolConfig.setFairness(false);

			// General configuration
			// This is the application owner part to configure

			// Pool max size
			jedisPoolConfig.setMaxTotal(100);
			// True - will block the thread requesting a connection from the pool
			// until a connection is ready (or until timeout - "MaxWaitMillis")
			// False - will immediately return an error
			jedisPoolConfig.setBlockWhenExhausted(true);
			// The maximum amount of time (in milliseconds) the borrowObject()
			// method should block before throwing an exception when the pool is
			// exhausted and getBlockWhenExhausted() is true.
			// When less than 0, the borrowObject() method may block indefinitely.
			jedisPoolConfig.setMaxWaitMillis(-1L);
			// The following EvictionRun parameters must be enabled (positive
			// values) in order to enable the evictor thread.
			// The number of milliseconds to sleep between runs of the idle object
			// evictor thread.
			// When positive, the idle object evictor thread starts.
			// Recommendation (>0) A good start is 1000 (one second)
			jedisPoolConfig.setTimeBetweenEvictionRunsMillis(1000L);
			// Number of conns to check each eviction run. Positive value is
			// absolute number of conns to check,
			// negative sets a portion to be checked ( -n means about 1/n of the
			// idle connections in the pool will be checked)
			// Recommendation (!=0) A good start is around fifth.
			jedisPoolConfig.setNumTestsPerEvictionRun(-5);

			JRedisTimeSeriesTest.config = jedisPoolConfig;

		}

		return config;
	}

	private static String getPoolCurrentUsage() {

		JedisPool jedisPool = getPoolInstance();
		JedisPoolConfig poolConfig = getPoolConfig();

		int active = jedisPool.getNumActive();
		int idle = jedisPool.getNumIdle();
		int total = active + idle;
		String log = String.format(
				"JedisPool: Active=%d, Idle=%d, Waiters=%d, total=%d, maxTotal=%d, minIdle=%d, maxIdle=%d", active,
				idle, jedisPool.getNumWaiters(), total, poolConfig.getMaxTotal(), poolConfig.getMinIdle(),
				poolConfig.getMaxIdle());

		return log;
	}

	/*
	private static class SimpleHostNameVerifier implements HostnameVerifier {

		private String exactCN;
		private String wildCardCN;

		public SimpleHostNameVerifier(String cacheHostname) {
			exactCN = "CN=" + cacheHostname;
			wildCardCN = "CN=*" + cacheHostname;
		}

		public boolean verify(String s, SSLSession sslSession) {
			try {
				String cn = sslSession.getPeerPrincipal().getName();
				return cn.equalsIgnoreCase(wildCardCN) || cn.equalsIgnoreCase(exactCN);
			} catch (SSLPeerUnverifiedException ex) {
				return false;
			}
		}

	}
	*/
}
