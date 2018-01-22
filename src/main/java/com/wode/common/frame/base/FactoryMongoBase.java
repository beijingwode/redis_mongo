package com.wode.common.frame.base;

import java.util.ResourceBundle;

import com.wode.common.mongo.MongoBaseDaoImpl;
import com.wode.common.mongo.MongoDBConfig;

public abstract class FactoryMongoBase<T> extends MongoBaseDaoImpl<T> {

	private static ResourceBundle res = ResourceBundle.getBundle("application");
	private static String host="localhost";
	private static int port=27017;
	private static String user="db1";
	private static String pwd="db123456";
	
	static {
		if(res.containsKey(MongoDBConfig.MONGO_SERVER_HOST)){
			host = res.getString(MongoDBConfig.MONGO_SERVER_HOST);
		}
		
		if(res.containsKey(MongoDBConfig.MONGO_SERVER_PORT)){
			port = Integer.parseInt(res.getString(MongoDBConfig.MONGO_SERVER_PORT));
		}
		
		if(res.containsKey(MongoDBConfig.MONGO_USER_NAME)){
			user = res.getString(MongoDBConfig.MONGO_USER_NAME);
		}
		
		if(res.containsKey(MongoDBConfig.MONGO_USER_PWD)){
			pwd = res.getString(MongoDBConfig.MONGO_USER_PWD);
		}
		
	}

    /** 
     * count 
     * <br>------------------------------<br> 
     * @param criteriaUser 
     * @return 
     */  
    public long count(BaseQuery criterDoc) { 
        return getMongoCollection().count(getFilter(query2Bean(criterDoc)));
    }
	public abstract T query2Bean(BaseQuery newdoc);
    
	@Override
	public String getHost() {
		return host;
	}

	@Override
	public String getDbName() {
		return "factory";
	}

	@Override
	public String getUserName() {
		return user;
	}

	@Override
	public String getPassword() {
		return pwd;
	}

	@Override
	public int getPort() {
		return port;
	}
}
