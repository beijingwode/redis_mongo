package com.wode.common.frame.base;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;

import com.wode.common.db.DBUtils;
import com.wode.common.exception.SystemException;
/**
 * @author badqiu
 * @version 1.0
 */
public abstract class FactoryBaseDaoImpl<E> extends SqlSessionDaoSupport implements FactoryBaseDao<E> {
    protected final Log log = LogFactory.getLog(getClass());
    @Autowired
    DBUtils dbUtils;
	@Autowired
	@Qualifier("sqlSessionTemplate") //to set other datasource
    public void setSqlSessionTemplate(SqlSessionTemplate sqlSessionTemplate) {
        super.setSqlSessionTemplate(sqlSessionTemplate);	
    }	
	
    protected <S> S getMapper(Class<S> clazz) {	
        return getSqlSession().getMapper(clazz);	
    }

	@Override
	public List<E> selectByModel(E model){
		return getSqlSession().selectList(getIbatisMapperNamesapce()+".selectByModel", model);
	}
	
    public E getById(Long primaryKey) {
    	
        E object = getSqlSession().selectOne(getFindByPrimaryKeyStatement(), primaryKey);
        return object;
    }
    
	public void deleteById(Long id) {
		getSqlSession().delete(getDeleteStatement(), id);
	}
	
    public E save(E entity) throws SystemException {
		prepareObjectForSaveOrUpdate(entity);
		getSqlSession().insert(getInsertStatement(), entity); 
		return entity;
    }
    
	public int update(E entity) {
		prepareObjectForSaveOrUpdate(entity);
		int affectCount = getSqlSession().update(getUpdateStatement(), entity);
		return affectCount;
	}
	

	@Override
	public void saveOrUpdate(E entity) throws DataAccessException {
		throw new UnsupportedOperationException();		
	}
	
	/**
	 * 用于子类覆盖,在insert,update之前调用
	 * @param o
	 */
    protected void prepareObjectForSaveOrUpdate(E o) {
    }

    public abstract String getIbatisMapperNamesapce();
    
    public String getFindByPrimaryKeyStatement() {
        return getIbatisMapperNamesapce()+".getById";
    }

    public String getInsertStatement() {
        return getIbatisMapperNamesapce()+".insert";
    }

    public String getUpdateStatement() {
    	return getIbatisMapperNamesapce()+".update";
    }

    public String getDeleteStatement() {
    	return getIbatisMapperNamesapce()+".delete";
    }

    public String getCountStatementForPaging(String statementName) {
		return statementName +"_count";
	}

	public List<E> findAll() {
		//throw new UnsupportedOperationException();
		return getSqlSession().selectList(getIbatisMapperNamesapce()+".findAll");
	}

	public boolean isUnique(E entity, String uniquePropertyNames) {
		throw new UnsupportedOperationException();
	}
	
	public void flush() {
		//ignore
	}
	
	public static interface SqlSessionCallback {
		
		public Object doInSession(SqlSession session);
		
	}
	
	
}
