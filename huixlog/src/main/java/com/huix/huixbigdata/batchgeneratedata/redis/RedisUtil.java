package com.huix.huixbigdata.batchgeneratedata.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis工具�?
 * 
 * @author mxlee
 *
 */
public class RedisUtil {
	protected static Logger logger = LoggerFactory.getLogger(JedisUtil.class);
	public static final String HOST = "127.0.0.1";
	public static final int PORT = 6379;

	private RedisUtil() {
	}

	private static JedisPool jedisPool = null;

	/**
	 * 初始化JedisPool
	 * 
	 * @return
	 */
	private static void initialPool() {

		if (jedisPool == null) {
			JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
			// 指定连接池中�?大的空闲连接�?
			jedisPoolConfig.setMaxIdle(100);
			// 连接池创建的�?大连接数
			jedisPoolConfig.setMaxTotal(500);
			// 设置创建连接的超时时�?
			jedisPoolConfig.setMaxWaitMillis(1000 * 50);
			// 表示从连接池中获取连接时，先测试连接是否可用
			jedisPoolConfig.setTestOnBorrow(true);
			jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT);
		}

	}

	/**
	 * 在多线程环境同步初始�?
	 */
	private static synchronized void poolInit() {
		if (jedisPool == null) {
			initialPool();
		}
	}

	/**
	 * 同步获取Jedis实例
	 * 
	 * @return Jedis
	 */
	public synchronized static Jedis getJedis() {
		if (jedisPool == null) {
			poolInit();
		}
		Jedis jedis = null;
		try {
			if (jedisPool != null) {
				jedis = jedisPool.getResource();
			}
		} catch (Exception e) {
			logger.error("获取jedis出错: " + e);
		} finally {
			returnResource(jedis);
		}
		return jedis;
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public static void returnResource(Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			// Jedis3.0之后，returnResource遭弃用，官方重写了close方法
			// jedisPool.returnResource(jedis);
			jedis.close();
		}
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public static void returnBrokenJedis(Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			jedisPool.returnBrokenResource(jedis);
		}
		jedis = null;
	}

}
