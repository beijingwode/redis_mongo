package com.wode.common.redis;

import org.springframework.beans.factory.annotation.Autowired;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by zoln on 2015/8/10.
 * Jedis客户端管理类,用来控制每个线程中redis的连接次数.
 *
 * 此类暂未启用
 */
//@Component
public class JedisHolder {

	private static JedisPool jedisPool;

	private static final ThreadLocal<Jedis> _jedis = new ThreadLocal();

	public static Jedis getJedis() {
		Jedis jedis = _jedis.get();
		if (jedis == null) {
			jedis = jedisPool.getResource();
			_jedis.set(jedis);
		}
		return jedis;
	}

	public static void destroy() {
		_jedis.get().close();
		_jedis.remove();
	}

	@Autowired
	public void setJedisPool(JedisPool jedisPool) {
		JedisHolder.jedisPool = jedisPool;
	}

}
