package com.wode.common.redis;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * 
 * <pre>
 * 功能说明: redis负载均衡客户端
 * 日期:	2015年5月5日
 * 开发者:	宋艳垒
 * 
 * 历史记录
 *    修改内容：
 *    修改人员：
 *    修改日期： 2015年5月5日
 * </pre>
 */
public class RedisUtilWeight {
	

	@Autowired
	private RoundRobinWeight roundRobinWeight;
	
	@Autowired
	private ShardedJedisPool shardedJedisPool;
	
	public RedisUtilWeight(){
		
	}
	
	
	public boolean setData(String key,String value,int seconds) {
		ShardedJedis redis = null;
		try {                  
			redis = shardedJedisPool.getResource();
			//Pipeline pip = redis.pipelined();
			redis.set(key, value);
			redis.expire(key, seconds);
			//pip.sync();
			return true;              
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {
			 
		}
		return false;      
	}
	
	public boolean setData(String key,String value) {
		ShardedJedis redis = null;
		try {                  
			//ShardedJedis jedis=getConnectionEx();   
			redis = shardedJedisPool.getResource();
			redis.set(key,value);                  
			return true;              
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			 
		}       
		return false;      
	}
	public boolean setData(String key,Map<String,String> map,int seconds) {
		ShardedJedis redis = null;
		try {                  
			redis = shardedJedisPool.getResource();
			//Pipeline pip = redis.pipelined();
			redis.hmset(key, map);
			redis.expire(key, seconds);
			//pip.sync();
			return true;              
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {
			 
		}
		return false;      
	}
	
	/**
	 * 插入map时不设置过期时间
	 * @param redis
	 */
	public boolean setData(String key,Map<String,String> map) {
		ShardedJedis redis = null;
		try {
			redis = shardedJedisPool.getResource();
			String ret=redis.hmset(key, map);
			return true;              
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {
		}
		return false;      
	}
	
	public boolean setMapData(String key,String field,String value) {
		ShardedJedis jedis = null;
		try {
			jedis = shardedJedisPool.getResource();
			jedis.hset(key, field, value);
			 return true;
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return false;      
	}
	
	
	public String getData(String key) {
		String value = null;              
		Jedis jedis = null;
		try {
//			ShardedJedis jedis=getConnectionEx();
			jedis = roundRobinWeight.getRedisSource(key);
			value=jedis.get(key);
			return value;              
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return value;      
	}
	
	public Map<String,String> getMap(String key) {
		Jedis jedis = null;
		try {
			jedis = roundRobinWeight.getRedisSource(key);
			return jedis.hgetAll(key);
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return null;      
	}
	
	
	public String getMapData(String key,String field) {
		Jedis jedis = null;
		try {
			jedis = roundRobinWeight.getRedisSource(key);
			return jedis.hget(key, field);
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {
		}
		return null;      
	}
	
	/**
	 * pipline 批量查询
	 * @param key
	 * @param fields
	 * @return
	 */
	public List<String> getMapData(String key,String... fields) {
		Jedis jedis = null;
		List<String> ret=new ArrayList();
		
		try {
			jedis = roundRobinWeight.getRedisSource(key);
			Pipeline pl = jedis.pipelined();
			for(String f:fields){
				ret.add(jedis.hget(key, f));
			}
			pl.sync();
			return ret;
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return null;      
	}
	
	public List<String> getDatas(String key[]) {
		Jedis jedis = null;
		List<String> ret=new ArrayList();
		try {
			Pipeline pl = null;
			for(String f:key){
				jedis = roundRobinWeight.getRedisSource(f);
				 pl = jedis.pipelined();
				ret.add(jedis.get(f));
			}
			pl.sync();
			return ret;
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return null;      
	}
	
	public List<String> getMapDatas(String key[],String field) {
		Jedis jedis = null;
		List<String> ret=new ArrayList();
		try {
			Pipeline pl = null;
			for(String f:key){
				jedis = roundRobinWeight.getRedisSource(f);
				pl = jedis.pipelined();
				ret.add(jedis.hget(f, field));
			}
			pl.sync();
			return ret;
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return null;      
	}
	
	
	public void addToSet(String setName,String value) {
		Jedis jedis = null;
		try {
			jedis = roundRobinWeight.getRedisSource(setName);
			jedis.sadd(setName,value);                 
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
	}
	
	public void removeSet(String setName,String key) {
		Jedis jedis = null;
		try {
			jedis = roundRobinWeight.getRedisSource(key);
			jedis.srem(setName,key);
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
	}

	public boolean ismember(String setName,String key) {
		Jedis jedis = null;
		try {
			jedis = roundRobinWeight.getRedisSource(key);
			return jedis.sismember(setName,key);
		} catch (Exception e) {
			e.printStackTrace();                                
		} finally {      
		}
		return false;
	}

	
	public boolean setList(String key,List list) {
		Jedis redis = null;
		try {                  
			redis = roundRobinWeight.getRedisSource(key);
			redis.set(key.getBytes(), serialize(list));  
			return true;              
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			 
		}       
		return false;      
	}
	
	 public byte[] serialize(Object value) {  
         if (value == null) {  
             throw new NullPointerException("Can't serialize null");  
         }  
         byte[] rv=null;  
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
	 

		public Object get(String key){
			Jedis jedis = null;
			try {
				jedis = roundRobinWeight.getRedisSource(key);
				Object value = jedis.get(key);
				return value;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}finally{
			}
		}
		
		
		public boolean del(String key){
			Jedis jedis = null;
			try {
				jedis = roundRobinWeight.getRedisSource(key);
				jedis.del(key);
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}finally{
			}
		}
		
} 

