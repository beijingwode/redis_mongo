package com.wode.common.mongo;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.wode.common.util.StringUtils;

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
public abstract class MongoBaseDaoImpl<T> implements MongoBaseDao<T> {
	public abstract String getCollName();

	public abstract T initBean(Document doc);
	public abstract Document toDocment(T obj);

	private int port=27017;
	private MongoCollection<Document> collection;
	
	public abstract String getHost();
	public abstract String getDbName();
	public abstract String getUserName();
	public abstract String getPassword();

	public int getPort() {
		return port;
	}

	public void setPort(int _port) {
		port = port;
	}

	protected MongoCollection<Document> getMongoCollection() {
		if(collection == null) {

	        MongoDBConfig.Builder builder = new MongoDBConfig.Builder();
	        builder.setIp(getHost())
	        	.setPort(getPort())
	        	.setSource(getDbName())
	        	.setUserName(getUserName())
	        	.setPassword(getPassword());
	        collection = MongoDBUtil.getCollection(builder.build(), getCollName());
		}
		return collection;
	}
	
	public abstract Bson getFilter(T newdoc);
	public Bson getIdFilter(String id){
        try {
        	return Filters.eq("_id",new ObjectId(id));
        } catch (Exception e) {
        	return Filters.eq("_id","-1");
        }
	}
	

	public void insert(T document) {
		getMongoCollection().insertOne(toDocment(document));
	}

	public long delete(String id) {
		DeleteResult del= getMongoCollection().deleteOne(getIdFilter(id));
		return del.getDeletedCount();
	}
	  /**
     * FIXME
     * 
     * @param coll
     * @param id
     * @param newdoc
     * @return
     */
    public long updateById(String id, T newdoc) {
		UpdateResult rst= getMongoCollection().updateOne(getIdFilter(id), toDocment(newdoc));
		return rst.getModifiedCount();
    }
    
    /** 
     * 根据条件查询出来后 再去修改 
     * <br>------------------------------<br> 
     * @param criteriaUser  查询条件 
     * @param updateUser    修改的值对象 
     * @return 
     */  
    public long findAndModify(T criterDoc, T newdoc) {  
    	
    	UpdateResult rst = getMongoCollection().updateOne(getFilter(criterDoc),toDocment(newdoc));
		return rst.getModifiedCount();
    }
    
	
	
    /** 
     * count 
     * <br>------------------------------<br> 
     * @param criteriaUser 
     * @return 
     */  
    public long count(T criterDoc) { 
        return getMongoCollection().count(getFilter(criterDoc));
    }  
    /** 
	 * 查询某表数据
	 * @author gzy
	 * @createDate 2015-12-18
	 * @param sort 排序字段
	 * @param sortType 排序方式（1升序，-1降序）
	 * @param collectionName 查询的collectionName（相当于表）
	 * @param skip限制查询的结果条数为skip条(小于1默认不限制)
	 * @param limit忽略匹配的前limit条（小于1默认不忽略）
	 */
    public List<T> find(T criteriaDoc, Bson orderBy, int pageNo, int pageSize) {
        if(pageNo > 0 && pageSize >0) {
        	if(orderBy == null) {
        		return convertList(getMongoCollection().find(getFilter(criteriaDoc)).skip((pageNo - 1) * pageSize).limit(pageSize));
        	} else {
        		return convertList(getMongoCollection().find(getFilter(criteriaDoc)).sort(orderBy).skip((pageNo - 1) * pageSize).limit(pageSize));
            }
        } else {

        	if(orderBy == null) {
        		return convertList(getMongoCollection().find(getFilter(criteriaDoc)));
        	} else {
        		return convertList(getMongoCollection().find(getFilter(criteriaDoc)).sort(orderBy));
            }
        }
    }
    public List<T> find(T criteriaDoc,String sort,int sortType, int pageNo, int pageSize) {
    	Bson orderBy = null;
        if(!StringUtils.isEmpty(sort)) {
        	orderBy = new BasicDBObject(sort, sortType);
        }
    	return this.find(criteriaDoc, orderBy, pageNo, pageSize);
    }
    public List<T> deCode(T criteriaDoc,String sort,int sortType, int pageNo, int pageSize) {
    	Bson orderBy = null;
        if(!StringUtils.isEmpty(sort)) {
        	orderBy = new BasicDBObject(sort, sortType);
        }
    	return this.find(criteriaDoc, orderBy, pageNo, pageSize);
    }
    /** 
     * 根据主键查询 
     * <br>------------------------------<br> 
     * @param id 
     * @return 
     */  
    public T findById(String id) {  
        return initBean(getMongoCollection().find(getIdFilter(id)).first());  
    }
    /** 
     * 查询全部 
     * <br>------------------------------<br> 
     * @return 
     */  
    public List<T> findAll() {
    	return convertList(getMongoCollection().find());
    }

    protected List<T> convertList(FindIterable<Document> ds) {
    	List<T> rst = new ArrayList<T>();
		for (Document document : ds) {
			rst.add(initBean(document));
		}
		return rst;
	}
}
