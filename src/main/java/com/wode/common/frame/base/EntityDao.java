package com.wode.common.frame.base;

import java.io.Serializable;
import java.util.List;

import org.springframework.dao.DataAccessException;
/**
 * @author badqiu
 */
public interface EntityDao <E,PK extends Serializable>{

	public E getById(PK id) throws DataAccessException;
	
	public void deleteById(PK id) throws DataAccessException;
	
	/** 插入数据 */
	public E save(E entity) throws DataAccessException;
	
	/** 更新数据 */
	public int update(E entity) throws DataAccessException;

	/** 根据id检查是否插入或是更新数据 */
	public void saveOrUpdate(E entity) throws DataAccessException;

	public boolean isUnique(E entity, String uniquePropertyNames) throws DataAccessException;
	
	public List<E> findAll() throws DataAccessException;

}
