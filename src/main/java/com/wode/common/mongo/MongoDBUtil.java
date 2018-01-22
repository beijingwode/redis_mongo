package com.wode.common.mongo;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * MongoDB工具类 Mongo实例代表了一个数据库连接池，即使在多线程的环境中，一个Mongo实例对我们来说已经足够了<br>
 * 注意Mongo已经实现了连接池，并且是线程安全的。 <br>
 * 设计为单例模式， 因 MongoDB的Java驱动是线程安全的，对于一般的应用，只要一个Mongo实例即可，<br>
 * Mongo有个内置的连接池（默认为10个） 对于有大量写和读的环境中，为了确保在一个Session中使用同一个DB时，<br>
 * DB和DBCollection是绝对线程安全的<br>
 * 
 * @author gaoyj(zhoulingfei)
 * @date 2015-5-29 上午11:49:49
 * @version 0.0.0
 * @Copyright (c)1997-2015 NavInfo Co.Ltd. All Rights Reserved.
 */
public class MongoDBUtil {

	/**
     * 定义一个枚举的元素，它代表此类的一个实例
     */
	
	private MongoClient mongoClient;

	private static Map<String,MongoCollection<Document>> map=new HashMap<String, MongoCollection<Document>>();
	

	private MongoDBUtil(MongoDBConfig config) {
		//===============MongoDBUtil初始化========================
		if(config.getCredentialsList() == null) {
			mongoClient= new MongoClient(config.getServerAddress(),config.getMongoClientOptions());	
		} else {
			mongoClient= new MongoClient(config.getServerAddress(),config.getCredentialsList(),config.getMongoClientOptions());
		}       
	}
	
	public static MongoDBUtil createInstance(MongoDBConfig config)  {
		return new MongoDBUtil(config);
	}

	public static MongoCollection<Document> getCollection(MongoDBConfig config,String collectionName)  {

		String key = "" + config.getServerAddress().getHost() + config.getServerAddress().getPort() + config.getDbName()+collectionName;
		if(map.containsKey(key)) {
			return map.get(key);
		} else {
			MongoDBUtil util = new MongoDBUtil(config);

			MongoCollection<Document> cc = util.mongoClient.getDatabase(config.getDbName()).getCollection(collectionName);
	        map.put(key, cc);
	        return cc;
	        
		}
	}

    // ------------------------------------共用方法---------------------------------------------------
    /**
     * 获取DB实例 - 指定DB
     * 
     * @param dbName
     * @return
     */
    protected MongoDatabase getDB(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            
            return database;
        }
        return null;
    }

    protected void dropCollection(String dbName, String collName) {
        getDB(dbName).getCollection(collName).drop();
    }

    /**
     * 关闭Mongodb
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
    
    /**
     * 测试入口
     * 
     * @param args
     */
    public static void main(String[] args) {

        String dbName = "factory";
        String collName = "access_log";

        MongoDBConfig.Builder builder = new MongoDBConfig.Builder();
        builder.setIp("106.2.210.170")
        	.setPort(27017)
        	.setSource("factory")
        	.setUserName("db1")
        	.setPassword("db123456");
        
        MongoDBUtil db = MongoDBUtil.createInstance(builder.build());
        
        
//        MongoCollection<Document> coll = db.getCollection(dbName, collName);
//
//         //插入多条
//         for (int i = 1; i <= 4; i++) {
//         Document doc = new Document();
//         doc.put("name", "zhoulf");
//         doc.put("school", "NEFU" + i);
//         Document interests = new Document();
//         interests.put("game", "game" + i);
//         interests.put("ball", "ball" + i);
//         doc.put("interests", interests);
//         coll.insertOne(doc);
//         }

        // // 根据ID查询
        // String id = "556925f34711371df0ddfd4b";
        // Document doc = MongoDBUtil2.instance.findById(coll, id);
        // System.out.println(doc);

        // 查询多个
//         MongoCursor<Document> cursor1 = coll.find(Filters.eq("name", "zhoulf")).iterator();
//         while (cursor1.hasNext()) {
//         org.bson.Document _doc = (Document) cursor1.next();
//         System.out.println(_doc.toString());
//         }
//         cursor1.close();

        // 查询多个
        // MongoCursor<Person> cursor2 = coll.find(Person.class).iterator();

        // 删除数据库
        // MongoDBUtil2.instance.dropDB("testdb");

        // 删除表
        // MongoDBUtil2.instance.dropCollection(dbName, collName);

        // 修改数据
        // String id = "556949504711371c60601b5a";
        // Document newdoc = new Document();
        // newdoc.put("name", "时候");
        // MongoDBUtil.instance.updateById(coll, id, newdoc);

        // 统计表
        // System.out.println(MongoDBUtil.instance.getCount(coll));
        
        // 查询所有
        Bson filter = Filters.eq("count", 0);
//        db.find(coll, filter);
    }
}
