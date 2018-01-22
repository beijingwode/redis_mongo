package com.wode.common.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Tuple;


/**
 * @author mengkaixuan
 * @see RedisUtil
 * @see <a href="http://redis.readthedocs.org/en/latest/index.html">http://redis.readthedocs.org/en/latest/index.html</a>
 * @since 1.2
 */
@Service("redisEx")
public class RedisUtilEx {

	private static Logger log = LoggerFactory.getLogger(RedisUtilEx.class);
	private static final String REDIS_SERVER_IP = "192.168.10.201";
	private static final Integer REDIS_SERVER_PORT = 6379;
	// 默认30分钟
	public static final int TIME_OUT_DEFAULT = 30 * 60;
	public static final String split = "_";// "."
	@Autowired
	@Qualifier("redis")
	private RedisUtil edis;


	public boolean setnx(String key, String value, int expire) {
		JedisCluster redis = edis.getConnection();
		redis.setnx(key, value);
		if (expire > 0)
			redis.expire(key, expire);
		return true;
	}


	public boolean addListAndString(String key, String pk, String value, String suffix) {
		JedisCluster redis = edis.getConnection();
		redis.sadd(key, pk);
		redis.set(pk + "." + key + suffix, value);
		return true;
	}

	public boolean removeListAndString(String key, String pk, String suffix) {
		JedisCluster redis = edis.getConnection();
		redis.srem(key, pk);
		redis.del(pk + "." + key + suffix);
		return true;
	}

	/**
	 * 根据key返回set集合
	 *
	 * @param key
	 * @return Set
	 */
	public Set<String> smembers(String key) {
		JedisCluster redis = edis.getConnection();
		return redis.smembers(key);
	}

	/**
	 * @param key
	 * @return
	 */
	public Set<Tuple> zrevrangeByScore(String key, Long start, Long end) {
		JedisCluster redis = edis.getConnection();
		return redis.zrevrangeWithScores(key, start, end);
	}

	public Long zscore(String key, String member) {
		JedisCluster redis = edis.getConnection();
		Double db = redis.zscore(key, member);
		if (db == null) return 0L;
		return Long.parseLong(new java.text.DecimalFormat("0").format(db));
	}

	public Long zscoreNull(String key, String member) {
		JedisCluster redis = edis.getConnection();
		Double db = redis.zscore(key, member);
		return db != null ? db.longValue() : null;
	}

	public Long incrby(String key, int integer) {
		JedisCluster redis = edis.getConnection();
		//redis.zadd(key, score, member);
		Long db = redis.incrBy(key, integer);
		return db;
	}

	public Long zincrby(String key, String member) {
		JedisCluster redis = edis.getConnection();
		//redis.zadd(key, score, member);
		Double db = redis.zincrby(key, 1, member);
		return Long.parseLong(new java.text.DecimalFormat("0").format(db));
	}

	/**
	 * 追加一个sortset
	 *
	 * @param key
	 * @param member
	 * @param score
	 * @return
	 */
	public boolean zadd(String key, String member, int score) {
		JedisCluster redis = edis.getConnection();
		redis.zadd(key, score, member);
		return true;
	}

	public boolean hset(String key, String field, String value) {
		JedisCluster redis = edis.getConnection();
		redis.hset(key, field, value);
		return true;
	}

	/**
	 * 获得个数
	 *
	 * @param key
	 * @param score
	 * @return
	 */
	public Long zcount(String key, int score) {
		JedisCluster redis = edis.getConnection();
		return redis.zcount(key, score, score);
	}

	/**
	 * 记录搜索结果
	 *
	 * @param key
	 * @param value
	 * @param seconds
	 * @return
	 */
	public boolean SetSearchResult(String key, String value, int seconds) {
		JedisCluster redis = edis.getConnection();
		redis.set(key, value);
		if (seconds > 0)
			redis.expire(key, seconds);
		return true;
	}

	public boolean SetSearchResultEx(String key, byte[] value, int seconds) {
		JedisCluster redis = edis.getConnection();
		redis.set(key.getBytes(), value);
		//当过期时间大于0时才
		if (seconds > 0)
			redis.expire(key, seconds);
		return true;
	}

	public String GetSearchResult(String key) {
		return this.getSearchResult(key, 0);
//		Jedis redis = null;
//		try {
//			redis = edis.getConnection();
//			return redis.get(key);
//		} catch (Exception e) {
//			log.error(e.getMessage(),e);
//			return null;
//		} finally {
//			edis.returnResource(redis);
//		}
	}

	public String getSearchResult(String key, int db) {
		JedisCluster redis = edis.getConnection();
		if (db > 0)
			redis.select(db);
//            Pipeline pip = redis.pipelined();
//            if(db != 0)
//                pip.select(db);
//            pip.get(key);
//            List<Object> list= pip.syncAndReturnAll();
		return redis.get(key);
	}


	public byte[] getSearchResultEx(String key, int db) {
		JedisCluster redis = edis.getConnection();
		if (db > 0)
			redis.select(db);
		return redis.get(key.getBytes());
	}

	public byte[] GetSearchResultEx(String key) {
		return this.getSearchResultEx(key, 0);
	}

	// TODO need to save another list to sort
	public List<Object> searchListByHash(String key) {
		// TODO
		JedisCluster redis = edis.getConnection();
		return (List<Object>) redis.hgetAll(key);
	}

	public Map<String, String> getMap(String key) {
		JedisCluster redis = edis.getConnection();
		return redis.hgetAll(key);
	}


	protected long getKeyLength(String key) {
		JedisCluster redis = edis.getConnection();
		long lengthb = redis.llen(key);
		return lengthb;
	}


	public void setSortSet(String key, double score, String mem) {
		JedisCluster redis = edis.getConnection();
//		boolean ret = redis.exists(key);
		redis.zadd(key, score, mem);
	}

	/**
	 * @param key
	 * @param score
	 * @return
	 */
	public List<String> getSortSet(String key, double score) {
		JedisCluster redis = edis.getConnection();
		Set<String> set = redis.zrangeByScore(key, score + "", score + "");
		List<String> list = new ArrayList<String>();
		list.addAll(set);
		return list;
	}

	/**
	 * @param key
	 * @return
	 */
	public boolean isExists(String key) {
		JedisCluster redis = edis.getConnection();
		boolean ret = redis.exists(key);
		return ret;
	}


	/**
	 * @param key
	 * @param member
	 * @return Double score
	 */
	public Double getScoreByMember(String key, String member) {
		Double d = 0D;
		JedisCluster redis = edis.getConnection();
		d = redis.zscore(key, member);
		if (d == null) {
			d = 0D;
		}
		return d;

	}


	/**
	 * bgsave
	 */
	public void saveDB() {
		JedisCluster redis = edis.getConnection();
		redis.bgsave();
	}

	/**
	 * @param dbIndex
	 */
	public void selectDB(int dbIndex) {
		JedisCluster redis = edis.getConnection();
		redis.select(dbIndex);
	}

	/**
	 *
	 */
	public void flushDB() {
		JedisCluster redis = edis.getConnection();
		redis.flushDB();
	}

	public void setEdis(RedisUtil edis) {
		this.edis = edis;
	}

	public RedisUtil getEdis() {
		return edis;
	}

	/**
	 * @param key
	 * @param value
	 */
	public void delFromList(String key, String value) {
		JedisCluster redis = edis.getConnection();
		redis.lrem(key, -1, value);
	}

	/**
	 * @param key
	 * @param field
	 */
	public void delFromHash(String key, String field) {
		JedisCluster redis = edis.getConnection();
		if (field != null) {
			redis.hdel(key, field);
		}
	}

	/**
	 * 删除key
	 *
	 * @param key
	 */
	public void delKey(String key) {
		JedisCluster redis = edis.getConnection();
		redis.exists(key);
		if (redis.exists(key)) {
			redis.del(key);
		}
	}

	/**
	 * 获取list
	 * LRANGE key start stop
	 *
	 * @param key   key倄1�7
	 * @param start 弄1�7始位置（仄1�7�1�7始）
	 * @param end   结束位置＄1�7-1代表倒数第一个，-2代表倒数第二个）
	 * @return 返回列表
	 */
	public List<String> lrangeList(String key, long start, long end) {
		if (null == key || "".equals(key)) {
			return null;
		}
		final JedisCluster redis = edis.getConnection();
		return redis.lrange(key, start, end);
	}

	/**
	 * 添加丄1�7个K,V到list末尾
	 * RPUSH key value [value ...]
	 *
	 * @param key
	 * @param values
	 * @return
	 */
	public Long rpushList(String key, String... values) {
		if (null == key) {
			return null;
		}
		final JedisCluster redis = edis.getConnection();
		return redis.rpush(key, values);
	}

	/**
	 * 删除key
	 *
	 * @param keys 丄1�7个或者多个key倄1�7
	 * @return 被删附1�7 key 的数釄1�7
	 */
	public Long delKey(String... keys) {
		final JedisCluster redis = edis.getConnection();
		return redis.del(keys);
	}

	/**
	 * HMSET key field value [field value ...]
	 *
	 * @param key  key
	 * @param hash Map<field, value>
	 */
	public String hmsetHash(String key, Map<String, String> hash) {
		final JedisCluster redis = edis.getConnection();
		return redis.hmset(key, hash);
	}

	/**
	 * 返回哈希衄1�7 key 中，丄1�7个或多个给定域的值�1�7�1�7
	 * HMGET key field [field ...]
	 *
	 * @param key    key倄1�7
	 * @param fields field倄1�7
	 * @return
	 */
	public List<String> hmgetHash(String key, String... fields) {
		final JedisCluster redis = edis.getConnection();
		return redis.hmget(key, fields);
	}

	/**
	 * @param key     key
	 * @param seconds
	 */
	public void expire(String key, int seconds) {
		JedisCluster redis = edis.getConnection();
			redis.expire(key, seconds);
	}


//	public Set<String> smembers(String key) {
//		final Jedis jedis = edis.getConnection();
//		Set<String> set = jedis.smembers(key);
//		edis.returnResource(jedis);
//		return set;
//	}

	/**
	 * 返回set集合长度
	 *
	 * @param key
	 * @return Long
	 */
	public Long scard(String key) {
		final JedisCluster redis = edis.getConnection();
		Long length = redis.scard(key);
		return length;
	}

	/**
	 * 返回set集合中key值对应的rank
	 *
	 * @param key
	 * @param member
	 * @return Long
	 */
	public Long zrank(String key, String member) {
		final JedisCluster redis = edis.getConnection();
		Long rank = redis.zrank(key, member);
		return rank;
	}
}
