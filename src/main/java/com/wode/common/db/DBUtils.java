package com.wode.common.db;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.wode.common.redis.RedisUtil;
import redis.clients.jedis.JedisCluster;

/**
 * DB基础类
 * @author mengkaixuan
 *
 */
@Service("dbUtils")
public class DBUtils {
	
	@Qualifier("redis")
	@Autowired
	private RedisUtil redisUtil;
	private final static long twepoch = 1425082458070L;
	
	 private final static int workerIdBits = 5;
	 public final static long maxWorkerId = -1L ^ -1L << workerIdBits;
	 private final static int sequenceBits = 10;
	 public final static long maxSequence = -1L ^ -1L << sequenceBits;

     private final static long workerIdShift = sequenceBits;

     private final static int timestampLeftShift = sequenceBits + workerIdBits;

	 
	private static AtomicLong sid = new AtomicLong(1);
	
	private  int workerid= 1;
	
	public DBUtils(){
	}
	//private static DBUtils dbUtils = null;
	
	public DBUtils(int workerId){
		if (workerId > this.maxWorkerId || workerId < 0) {
		   throw new IllegalArgumentException(String.format(
		     "worker Id can't be greater than %d or less than 0",
		     this.maxWorkerId));
		  }
		  this.workerid = workerId;
	}

	
	/**
	 *  
	 * @return
	 */
	private long IncID(){
		Long id = 1l;
		JedisCluster redis = null;
        try {
            redis = redisUtil.getConnection();
            id = redis.incr("AutoID");
            if(id >= maxSequence){
                redis.set("AutoID", "0");
            }
        }catch (Exception e){
            sid.addAndGet(1);
            if(sid.get() > maxSequence)sid=  new AtomicLong(0);
            return sid.get();
        }
		return id;
	}
	
	
	/**
	 * 获得唯一ID 数据库的主键
     * 
	 * @return
	 */
	public long CreateID(){
		long iid = this.IncID();		
		long id = System.currentTimeMillis()-twepoch;		
		id = (id<<timestampLeftShift)|(workerid<<workerIdShift)|iid;
		return id;
	}
	
	
	public static void main(String[] args) {
		DBUtils dbu=new DBUtils();
		System.out.println(dbu.CreateID());
		
		Long l=System.currentTimeMillis();
		l=l-3600*24*365;
		
		System.out.println(System.currentTimeMillis()-twepoch);
	}
}
