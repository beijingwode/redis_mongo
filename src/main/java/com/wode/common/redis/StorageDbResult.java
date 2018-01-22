package com.wode.common.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.wode.common.stereotype.QueryCached;
import com.wode.common.util.JsonUtil;
import com.wode.common.util.StringUtils;




/**
 * 
 * record mysql data to redis
 * 
 * @author mengkaixuan
 * 
 */
public class StorageDbResult implements MethodInterceptor {// ,Advice{
// ,AfterReturningAdvice {

	private static Logger logger = LoggerFactory.getLogger(StorageDbResult.class);
	@Autowired
	@Qualifier("redisEx")
	private RedisUtilEx redisUtilEx;

	/**
	 * aop record data to redis
	 * when you query some data,return data if that is in redis else <br>
	 * this will query in mysql and record result in redis
	 */
	public Object invoke(MethodInvocation invocation) throws Throwable {

		String className = invocation.getThis().getClass().getName();// 

		Object[] args = invocation.getArguments();// get param
		QueryCached replication = null;//
		
		if (invocation.getMethod().isAnnotationPresent(QueryCached.class)) {//
			replication = invocation.getMethod().getAnnotation(QueryCached.class);
		} else {//call 
			return invocation.proceed();
		}
		String key = "";
        if(!StringUtils.isNullOrEmpty(replication.keyPreFix())){
             key = replication.keyPreFix() + "_" +JsonUtil.toJsonString(args);
        }
        else if(!StringUtils.isNullOrEmpty(replication.key())){
             key =  replication.key();
        }
        else{
            key = className+"_"+invocation.getMethod().getName()+"_"+ JsonUtil.toJsonString(args);
        }
		byte[] byret = redisUtilEx.GetSearchResultEx(key);
		
		if(byret != null){
			try{
				ByteArrayInputStream bt = new ByteArrayInputStream(byret);
				ObjectInputStream ois = new ObjectInputStream(bt);
				Object ret =  ois.readObject();
				ois.close();
				bt.close();
				logger.info("$$  " + invocation.getMethod() + " be executed.   " + key);
				return ret;
			}catch(Exception e){
                logger.error(e.getLocalizedMessage());
				return invocation.proceed();
			}
					
		}else{
			Object ret = invocation.proceed();
			try{
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				ObjectOutputStream oos=new ObjectOutputStream(os);
			    oos.writeObject(ret);
			    oos.close();
			    byte[] bt = os.toByteArray();
				logger.info("$$  " + invocation.getMethod() + " save cache.   " + key);
			    redisUtilEx.SetSearchResultEx(key, bt, replication.timeout());
			    os.close();
			}
			catch(Exception e){
                logger.error(e.getLocalizedMessage());
			}			
			return ret;			
		}		
	}
}
