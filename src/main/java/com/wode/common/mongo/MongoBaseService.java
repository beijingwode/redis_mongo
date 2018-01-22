package com.wode.common.mongo;

import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
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
public abstract class MongoBaseService<T> {
	public abstract MongoBaseDao<T> getMongoDao();

	public void insert(T document) {
		getMongoDao().insert(document);
	}

	public long delete(String id) {
		return getMongoDao().delete(id);
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
		return getMongoDao().updateById(id, newdoc);
    }
    
    /** 
     * 根据条件查询出来后 再去修改 
     * <br>------------------------------<br> 
     * @param criteriaUser  查询条件 
     * @param updateUser    修改的值对象 
     * @return 
     */  
    public long findAndModify(T criterDoc, T newdoc) {  
		return getMongoDao().findAndModify(criterDoc, newdoc);
    }
    
    /** 
     * count 
     * <br>------------------------------<br> 
     * @param criteriaUser 
     * @return 
     */  
    public long count(T criterDoc) { 
    	return getMongoDao().count(criterDoc);
    }  

    /** 
     * 根据主键查询 
     * <br>------------------------------<br> 
     * @param id 
     * @return 
     */  
    public T findById(String id) {  
    	return getMongoDao().findById( id);
    }
    /** 
     * 查询全部 
     * <br>------------------------------<br> 
     * @return 
     */  
    public List<T> findAll() {
    	return getMongoDao().findAll();
    }

    /** 
	 * 查询某表数据
	 * @author gaoyj
	 * @createDate 2015-12-18
	 * @param sort 排序字段
	 * @param sortType 排序方式（1升序，-1降序）
	 * @param collectionName 查询的collectionName（相当于表）
	 * @param skip限制查询的结果条数为skip条(小于1默认不限制)
	 * @param limit忽略匹配的前limit条（小于1默认不忽略）
	 */
    public List<T> find(T criteriaDoc, int pageNo, int pageSize) {  
        return find(criteriaDoc,null,pageNo,pageSize); 
    }  
    public List<T> find(T criteriaDoc, String sort,int sortType, int pageNo, int pageSize) {  
    	Bson orderBy = null;
        if(!StringUtils.isEmpty(sort)) {
        	orderBy = new BasicDBObject(sort, sortType);
        }
        return find(criteriaDoc,orderBy,pageNo,pageSize);
    }  
    public List<T> find(T criteriaDoc, Bson orderBy, int pageNo, int pageSize) {
    	return getMongoDao().find(criteriaDoc, orderBy, pageNo, pageSize);
    } 
    public List<T> find(T criteriaDoc,String sort,int sortType) {  
        return find(criteriaDoc,sort,sortType,0,0); 
    }  
    public List<T> find(T criteriaDoc) {  
        return find(criteriaDoc,null,0,0); 
    }

}
