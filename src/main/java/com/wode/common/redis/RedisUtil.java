package com.wode.common.redis;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * @author mengkaixuan
 * @see <a href="http://redis.readthedocs.org/en/latest/index.html">http://redis.readthedocs.org/en/latest/index.html</a>
 */
@Service("redis")
public class RedisUtil {

	@Resource(name = "redisClusterPoolConfig")
	private GenericObjectPoolConfig redisClusterPoolConfig;

	private JedisCluster jedis;

	@Value("#{'${redis.servers}'.split(',')}")
	private List<String> redisServers;

	@PostConstruct
	public void initJedis() {
		Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");
		if (redisServers == null || redisServers.size() == 0) {
			throw new RuntimeException("没有找到redis服务器相关配置,程序将无法正常运行!!!");
		} else {
			Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
			boolean isLegalServer;
			for (String server : redisServers) {
				isLegalServer = p.matcher(server).matches();
				if (isLegalServer) {
					String[] server_split = server.split(":");
					jedisClusterNodes.add(new HostAndPort(server_split[0], Integer.valueOf(server_split[1])));
				} else {
					throw new RuntimeException("redis服务配置错误,程序将无法正常运行!!!");
				}
			}
			jedis = new JedisCluster(jedisClusterNodes);
		}
	}

	public JedisCluster getConnection() {
		return jedis;
	}

	public boolean setData(String key, String value, int seconds) {
		jedis.set(key, value);
		jedis.expire(key, seconds);
		return true;
	}

	public boolean hincr(String key, String field, long value) {
		jedis.hincrBy(key, field, value);
		return true;
	}

	public boolean incr(String key, long value) {
		jedis.incrBy(key, value);
		return true;
	}


	public boolean setData(String key, String value) {
		jedis.set(key, value);
		return true;
	}

	public boolean setData(String key, Map<String, String> map, int seconds) {
		jedis.hmset(key, map);
		jedis.expire(key, seconds);
		return true;
	}

	/**
	 * 根据key设置过期时间
	 *
	 * @param key
	 * @param seconds
	 * @return
	 */
	public boolean setTime(String key, int seconds) {
		jedis.expire(key, seconds);
		return true;
	}

	/**
	 * 插入map时不设置过期时间
	 *
	 * @param
	 */
	public boolean setData(String key, Map<String, String> map) {
		jedis.hmset(key, map);
		return true;
	}

	public boolean setMapData(String key, String field, String value) {
		jedis.hset(key, field, value);
		return true;
	}

	public boolean push(String channel, String message) {
		jedis.publish(channel, message);
		return true;
	}

	public String getData(String key) {
		String value = jedis.get(key);
		return value;
	}

	public Map<String, String> getMap(String key) {
		return jedis.hgetAll(key);
	}


	public String getMapData(String key, String field) {
		return jedis.hget(key, field);
	}

	/**
	 * pipline 批量查询
	 *
	 * @param key
	 * @param fields
	 * @return
	 */
	public List<String> getMapData(String key, String... fields) {
		List<String> ret = new ArrayList();
		for (String f : fields) {
			ret.add(jedis.hget(key, f));
		}
		return ret;
	}

	public List<String> getDatas(String key[]) {
		List<String> ret = new ArrayList();
		for (String f : key) {
			ret.add(jedis.get(f));
		}
		return ret;
	}

	public List<Map> getMaps(String key[]) {
		List<Map> ret = new ArrayList();
		for (String f : key) {
			ret.add(jedis.hgetAll(f));
		}
		return ret;
	}


	public List<String> getMapDatas(String key[], String field) {
		List<String> ret = new ArrayList();
		for (String f : key) {
			ret.add(jedis.hget(f, field));
		}
		return ret;
	}


	public void addToSet(String setName, String value) {
		jedis.sadd(setName, value);
	}

//	public void removeFromSet(String setName, String... key) {
//		Jedis jedis = null;
//		jedis.srem(setName, key);
//	}

	public Set<String> getAllSet(String setName) {
		return jedis.smembers(setName);
	}

	public void rpush(String setName, String value) {
		jedis.rpush(setName, value);
	}

	public void lpush(String setName, String value) {
		jedis.lpush(setName, value);
	}

	public String lpop(String key) {
		return jedis.lpop(key);
	}

	public void removeSet(String setName, String key) {
		jedis.srem(setName, key);
	}

	public boolean ismember(String setName, String key) {
		return jedis.sismember(setName, key);
	}

	public String spop(String key) {
			return jedis.spop(key);
	}

	public boolean setList(String key, List list) {
		jedis.set(key.getBytes(), serialize(list));
		return true;
	}

	public byte[] serialize(Object value) {
		if (value == null) {
			throw new NullPointerException("Can't serialize null");
		}
		byte[] rv = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream os = null;
		try {
			bos = new ByteArrayOutputStream();
			os = new ObjectOutputStream(bos);
			os.writeObject(value);
			os.close();
			bos.close();
			rv = bos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException("Non-serializable object", e);
		} finally {
			close(os);
			close(bos);
		}
		return rv;
	}

	public void close(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
			}
		}
	}


	public Object get(String key) {
		Object value = jedis.get(key);
		return value;
	}


	public boolean del(String key) {
		jedis.del(key);
		return true;
	}

	public Long delMapData(String key, String... fields) {
		return jedis.hdel(key, fields);
	}
} 

