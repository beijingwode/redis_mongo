package com.wode.common.mongo;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.wode.common.util.StringUtils;

public class MongoDBConfig  {

	public static final String MONGO_SERVER_HOST = "mongo.server.host";
	public static final String MONGO_SERVER_PORT = "mongo.server.port";
	public static final String MONGO_USER_NAME = "mongo.user.name";
	public static final String MONGO_USER_PWD = "mongo.user.pwd";
	public static final String MONGO_USER_SOURCE = "mongo.user.source";
	
	
	private ServerAddress serverAddress;
	private List<MongoCredential> credentialsList;
	private MongoClientOptions mongoClientOptions;
	private String dbName;
	
    private MongoDBConfig(final Builder builder) {
    	MongoClientOptions.Builder options = new MongoClientOptions.Builder();
        // options.autoConnectRetry(true);// 自动重连true
        // options.maxAutoConnectRetryTime(10); // the maximum auto connect retry time
        options.connectionsPerHost(300);// 连接池设置为300个连接,默认为100
        options.connectTimeout(15000);// 连接超时，推荐>3000毫秒
        options.maxWaitTime(5000); //
        options.socketTimeout(0);// 套接字超时时间，0无限制
        options.threadsAllowedToBlockForConnectionMultiplier(5000);// 线程队列数，如果连接线程排满了队列就会抛出“Out of semaphores to get db”错误。
        options.writeConcern(WriteConcern.SAFE);//
        options.socketKeepAlive(false);
        
        mongoClientOptions=options.build();

        dbName=builder.source;
        serverAddress = new ServerAddress(builder.s_ip, builder.s_port);
        
        if(!StringUtils.isEmpty(builder.userName) && !StringUtils.isEmpty(builder.pwd)) {
        	MongoCredential credentials =MongoCredential.createScramSha1Credential(builder.userName, builder.source, builder.pwd.toCharArray());
        	credentialsList=new ArrayList<MongoCredential>();
            credentialsList.add(credentials);
        }
        
        
    }

    public String getDbName() {
		return dbName;
	}

    public ServerAddress getServerAddress() {
		return serverAddress;
	}

	public List<MongoCredential> getCredentialsList() {
		return credentialsList;
	}

	public MongoClientOptions getMongoClientOptions() {
		return mongoClientOptions;
	}

	public static class Builder {
    	private String s_ip ="106.2.210.170";
    	private int s_port =27017;
    	private String source = "factory";
    	private String userName;
    	private String pwd;
    	
    	public Builder setIp(String ip) {
    		this.s_ip = ip;
    		return this;
    	}

    	public Builder setPort(int port) {
    		this.s_port = port;
    		return this;
    	}
    	
    	public Builder setSource(String source) {
    		this.source = source;
    		return this;
    	}
    	
    	public Builder setUserName(String userName) {
    		this.userName = userName;
    		return this;
    	}
    	
    	public Builder setPassword(String password) {
    		this.pwd = password;
    		return this;
    	}
    	
        /**
         * Build an instance of MongoClientOptions.
         *
         * @return the options from this builder
         */
        public MongoDBConfig build() {
            return new MongoDBConfig(this);
        }
    }
}
