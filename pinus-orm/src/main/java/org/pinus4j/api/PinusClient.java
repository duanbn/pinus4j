package org.pinus4j.api;

import java.util.List;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;

/**
 * main api of pinus.
 * 
 * @author duanbn
 */
public interface PinusClient {

    public void save(Object entity);

    public void saveBatch(List<Object> entityList);

    public void update(Object entity);

    public void updateBatch(List<Object> entityList);

    public void delete(Object entity);

    public void delete(List<Object> entityList);

    public void load(Object entity);

    public void load(Object entity, boolean useCache);

    public void load(Object entity, EnumDBMasterSlave masterSlave);

    public void load(Object entity, boolean useCache, EnumDBMasterSlave masterSlave);

    public IQuery createQuery(Class<?> clazz);

    public <T> List<T> findBySQL(SQL sql);

}
