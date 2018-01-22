package com.wode.common.redis;
import java.util.Collection;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import com.wode.common.util.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedisPool;
/**
 * redis的Java客户端Jedis测试验证
 *
 * @author
 */
public class RedisBast {
	
	/**
	 * 切片链接池
	 */
	@Resource(name="shardedJedisPool")
	private ShardedJedisPool shardedJedisPool;
																	   
	
	public Jedis select(String key){
		if(shardedJedisPool!=null){
			Collection<Jedis> je = shardedJedisPool.getResource().getAllShards();
			for (Jedis jedis : je) {
				if(!StringUtils.isNullOrEmpty(jedis.get(key))){
					return jedis;
				}
			}
		}
		return null;
	}
	
	@PostConstruct
	public void initJedisPool(){
		this.setShardedJedisPool(shardedJedisPool);
	}


	public ShardedJedisPool getShardedJedisPool() {
		return shardedJedisPool;
	}


	public void setShardedJedisPool(ShardedJedisPool shardedJedisPool) {
		this.shardedJedisPool = shardedJedisPool;
	}
	
	
	
}