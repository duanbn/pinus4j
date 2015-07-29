/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.entity;

import java.util.List;

import org.pinus4j.entity.meta.DBTable;
import org.pinus4j.entity.meta.DBTablePK;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;

/**
 * Entity管理接口
 * 
 * @author shanwei Jul 22, 2015 1:38:37 PM
 */
public interface IEntityMetaManager {

    /**
     * 判断缓存是否开启
     * 
     * @param clazz
     * @return
     */
    public boolean isCache(Class<?> clazz);

    /**
     * 获取表名不带下标
     * 
     * @param clazz
     * @return
     */
    public String getTableName(Class<?> clazz);

    /**
     * 获取表名.
     * 
     * @param clazz
     * @param tableIndex
     * @return
     */
    public String getTableName(Class<?> clazz, int tableIndex);

    /**
     * 获取表名
     * 
     * @param entity
     * @param tableIndex 表下标
     * @return
     */
    public String getTableName(Object entity, int tableIndex);

    /**
     * 获取分片表数
     * 
     * @param clazz
     * @return
     */
    public int getTableNum(Class<?> clazz);

    /**
     * 获取集群名
     * 
     * @param clazz
     * @return
     */
    public String getClusterName(Class<?> clazz);

    /**
     * 获取分片值
     * 
     * @param entity
     * @return
     */
    public Object getShardingValue(Object entity);

    /**
     * 判断是否是分片
     * 
     * @param clazz
     * @return
     */
    public boolean isShardingEntity(Class<?> clazz);

    /**
     * 获取联合主键的主键名.
     * 
     * @param clazz
     * @return
     */
    public List<PKName> getPkName(Class<?> clazz);

    /**
     * 获取联合主键的主键名
     * 
     * @param clazz
     * @return
     */
    public PKName getNotUnionPkName(Class<?> clazz);

    /**
     * 获取主键信息，包括联合主键
     * 
     * @param obj
     * @return
     */
    public EntityPK getEntityPK(Object obj);

    /**
     * 获取非联合主键的主键值
     * 
     * @param obj
     * @return
     */
    public PKValue getNotUnionPkValue(Object obj);

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
