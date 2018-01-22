package com.wode.common.mongo;

import java.util.List;

import org.bson.conversions.Bson;

public interface MongoBaseDao<T> {

	public void insert(T document);

	public long delete(String id);

	public long updateById(String id, T newdoc);

	public long findAndModify(T criterDoc, T newdoc);

	public long count(T criterDoc);

	public T findById(String id);

	public List<T> findAll();

	/**
	 * 查询某表数据
	 * 
	 * @author gzy
	 * @createDate 2015-12-18
	 * @param sort
	 *            排序字段
	 * @param sortType
	 *            排序方式（1升序，-1降序）
	 * @param collectionName
	 *            查询的collectionName（相当于表）
	 * @param skip限制查询的结果条数为skip条
	 *            (小于1默认不限制)
	 * @param limit忽略匹配的前limit条
	 *            （小于1默认不忽略）
	 */
	public List<T> find(T criteriaDoc, Bson orderBy, int pageNo, int pageSize);
}
