package org.pinus4j.entity;

import java.util.List;

import org.pinus4j.entity.meta.DBTable;
import org.pinus4j.entity.meta.DBTablePK;

/**
 * Entity管理接口
 * 
 * @author shanwei Jul 22, 2015 1:38:37 PM
 */
public interface IEntityMetaManager {

    /**
     * 判断实体是否是联合主键
     * 
     * @param clazz
     * @return
     */
    public boolean isUnionKey(Class<?> clazz);

    /**
     * 获取实体的自增主键信息，如果没有则返回null;
     * 
     * @return
     */
    public DBTablePK getAutoIncrementField(Class<?> clazz);

    /**
     * 清理已经加载的@Table对象，并重新扫描
     * 
     * @param scanPackage
     */
    public void reloadEntity(String scanPackage);

    /**
     * 扫描classpath中的@Table对象，并加载到内存
     * 
     * @param scanPackage
     */
    public void loadEntity(String scanPackage);

    public List<DBTable> getTableMetaList();

    public DBTable getTableMeta(Class<?> entityClass);

}
