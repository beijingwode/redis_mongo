package com.wode.common.frame.base;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.wode.common.db.DBUtils;
import com.wode.common.exception.SystemException;
import com.wode.common.frame.base.page.PagerModel;
import com.wode.common.stereotype.PrimaryKey;

import cn.org.rapid_framework.beanutils.PropertyUtils;
import cn.org.rapid_framework.page.Page;
import cn.org.rapid_framework.page.PageRequest;
/**
 * @author badqiu
 * @version 1.0
 */
public abstract class BaseDao<E,PK extends Serializable> extends SqlSessionDaoSupport implements EntityDao<E,PK> {
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
    
    public E getById(PK primaryKey) {
    	
        E object = getSqlSession().selectOne(getFindByPrimaryKeyStatement(), primaryKey);
        return object;
    }
    
	public void deleteById(PK id) {
		int affectCount = getSqlSession().delete(getDeleteStatement(), id);
	}
	
    public E save(E entity) throws SystemException {
    	long pk = dbUtils.CreateID();
    	Field[] fileds = entity.getClass().getDeclaredFields();
    	for(Field f : fileds){
    		PrimaryKey pmk = f.getAnnotation(PrimaryKey.class);
    		if(pmk != null){
    			String fieldName=f.getName();   
                String stringLetter=fieldName.substring(0, 1).toUpperCase();   
                   
                //获得相应属性的getXXX和setXXX方法名称   
                //String getName="get"+stringLetter+fieldName.substring(1);   
                String setName="set"+stringLetter+fieldName.substring(1); 
                try {
					Method setMethod=entity.getClass().getMethod(setName, new Class[]{f.getType()});
					Object value=setMethod.invoke(entity,pk );   
				} catch (Exception e) {
					
				}
//    			f.setAccessible(true);
//    			try {
//					f.set(entity,(Object)pk);
//				} catch (IllegalArgumentException | IllegalAccessException e) {
//					throw new SystemException("system.err.unkown", e);
//				}
//    			f.setAccessible(false);
    			break;
    		}
    	}
		prepareObjectForSaveOrUpdate(entity);
		int affectCount = getSqlSession().insert(getInsertStatement(), entity); 
		return entity;
    }
    
	public int update(E entity) {
		prepareObjectForSaveOrUpdate(entity);
		int affectCount = getSqlSession().update(getUpdateStatement(), entity);
		return affectCount;
	}
	
	/**
	 * 用于子类覆盖,在insert,update之前调用
	 * @param o
	 */
    protected void prepareObjectForSaveOrUpdate(E o) {
    }

    public String getIbatisMapperNamesapce() {
        throw new RuntimeException("not yet implement");
    }
    
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

    /**
     * 
	 * @deprecated
     * @param statementName
     * @param pageRequest
     * @return
     */
	protected Page pageQuery(String statementName, PageRequest pageRequest) {
		return pageQuery(getSqlSession(),statementName,getCountStatementForPaging(statementName),pageRequest);
	}
	
	/**
	 * @deprecated
	 * @param sqlSessionTemplate
	 * @param statementName
	 * @param countStatementName
	 * @param pageRequest
	 * @return
	 */
	public static Page pageQuery(SqlSession sqlSessionTemplate,String statementName,String countStatementName, PageRequest pageRequest) {
		
		Number totalCount = (Number) sqlSessionTemplate.selectOne(countStatementName,pageRequest);
		if(totalCount == null || totalCount.longValue() <= 0) {
			return new Page(pageRequest,0);
		}
		
		Page page = new Page(pageRequest,totalCount.intValue());
		
		//其它分页参数,用于不喜欢或是因为兼容性而不使用方言(Dialect)的分页用户使用. 与getSqlMapClientTemplate().queryForList(statementName, parameterObject)配合使用
		Map filters = new HashMap();
		filters.put("offset", page.getFirstResult());
		filters.put("pageSize", page.getPageSize());
		filters.put("lastRows", page.getFirstResult() + page.getPageSize());
		filters.put("sortColumns", pageRequest.getSortColumns());
		
		Map parameterObject = PropertyUtils.describe(pageRequest);
		filters.putAll(parameterObject);
		List list = sqlSessionTemplate.selectList(statementName, filters,new RowBounds(page.getFirstResult(), page.getPageSize()) );
		page.setResult(list);
		return page;
	}
	
	/**
	 * 分页查询
	 * @param selectList
	 * @param selectCount
	 * @param param
	 * @return
	 */
	public PagerModel selectPageList(String selectList, String selectCount,Object param) {
		SqlSession session = getSqlSession();
		List list = session.selectList(selectList, param);
		PagerModel pm = new PagerModel();
		pm.setList(list);
		Object oneC = session.selectOne(selectCount, param);
		if(oneC!=null){
			pm.setTotal(Integer.parseInt(oneC.toString()));
		}else{
			pm.setTotal(0);
		}
		return pm;
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
