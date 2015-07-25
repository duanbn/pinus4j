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
package org.pinus4j.cache;

import java.util.List;
import java.util.Map;

import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.entity.meta.EntityPK;

/**
 * 主缓存接口. 为Pinus存储提供一级缓存, 一级缓存使用memcached作为存储主要是对数据库表中的数据进行缓存查询进行缓存.
 * 一级缓存的key格式：[clusterName + dbIndex].[tableName + tableIndex].id, value是数据库记录.
 * 删除、修改操作会清除被操作数据的缓存
 * 
 * @author duanbn
 */
public interface IPrimaryCache extends ICache {

    /**
     * 设置count数.
     * 
     * @param db 分库分表
     * @param count count数
     */
    public void setCountGlobal(String clusterName, String tableName, long count);

    /**
     * 删除count数.
     * 
     * @param db 分库分表
     */
    public void removeCountGlobal(String clusterName, String tableName);

    /**
     * 减少分表count数.
     * 
     * @param db 分库分表
     * @param delta 减少数
     * @return 减少后的count数
     */
    public long decrCountGlobal(String clusterName, String tableName, int delta);

    /**
     * 增加分表count数.
     * 
     * @param db 分库分表
     * @param delta 增加数
     * @return 增加后的count数
     */
    public long incrCountGlobal(String clusterName, String tableName, int delta);

    /**
     * 获取一张表的count值.
     * 
     * @param db 分库分表
     * @return count值.
     */
    public long getCountGlobal(String clusterName, String tableName);

    /**
     * 添加一条记录. 如果存在则替换.
     * 
     * @param db 分库分表
     * @param pk 主键
     * @param data 记录
     */
    public void putGlobal(String clusterName, String tableName, EntityPK pk, Object data);

    /**
     * 批量添加记录.
     * 
     * @param db 分库分表
     * @param data 批量数据
     */
    public void putGlobal(String clusterName, String tableName, List<? extends Object> data);

    /**
     * 批量添加记录
     * 
     * @param clusterName
     * @param tableName
     * @param data
     */
    public void putGlobal(String clusterName, String tableName, Map<EntityPK, ? extends Object> data);

    /**
     * 获取记录.
     * 
     * @param db 分库分表
     * @param pk 主键
     * @return 记录
     */
    public <T> T getGlobal(String clusterName, String tableName, EntityPK pk);

    /**
     * 获取多条记录.
     * 
     * @param db 分库分表
     * @param pks 主键
     * @return 多条数据
     */
    public <T> List<T> getGlobal(String clusterName, String tableName, EntityPK[] pks);

    /**
     * 删除一条记录.
     * 
     * @param db 分库分表
     * @param pk 主键
     */
    public void removeGlobal(String clusterName, String tableName, EntityPK pk);

    /**
     * 批量删除缓存.
     * 
     * @param db 分库分表
     * @param pks 主键
     */
    public void removeGlobal(String clusterName, String tableName, List<EntityPK> pks);

    /**
     * 设置count数.
     * 
     * @param db 分库分表
     * @param count count数
     */
    public void setCount(ShardingDBResource db, long count);

    /**
     * 删除count数.
     * 
     * @param db 分库分表
     */
    public void removeCount(ShardingDBResource db);

    /**
     * 减少分表count数.
     * 
     * @param db 分库分表
     * @param delta 减少数
     * @return 减少后的count数
     */
    public long decrCount(ShardingDBResource db, long delta);

    /**
     * 增加分表count数.
     * 
     * @param db 分库分表
     * @param delta 增加数
     * @return 增加后的count数
     */
    public long incrCount(ShardingDBResource db, long delta);

    /**
     * 获取一张表的count值.
     * 
     * @param db 分库分表
     * @return count值.
     */
    public long getCount(ShardingDBResource db);

    /**
     * 添加一条记录. 如果存在则替换.
     * 
     * @param db 分库分表
     * @param pk 主键
     * @param data 记录
     */
    public void put(ShardingDBResource db, EntityPK pk, Object data);

    /**
     * 批量添加记录.
     * 
     * @param db 分库分表
     * @param pks 主键
     * @param data 批量数据
     */
    public void put(ShardingDBResource db, EntityPK[] pks, List<? extends Object> data);

    /**
     * 批量添加记录.
     * 
     * @param db
     * @param data
     */
    public void put(ShardingDBResource db, Map<EntityPK, ? extends Object> data);

    /**
     * 获取记录.
     * 
     * @param db 分库分表
     * @param pk 主键
     * @return 记录
     */
    public <T> T get(ShardingDBResource db, EntityPK pk);

    /**
     * 获取多条记录.
     * 
     * @param db 分库分表
     * @param ids 主键
     * @return 多条数据
     */
    public <T> List<T> get(ShardingDBResource db, EntityPK[] ids);

    /**
     * 删除一条记录.
     * 
     * @param db 分库分表
     * @param pk 主键
     */
    public void remove(ShardingDBResource db, EntityPK pk);

    /**
     * 批量删除缓存.
     * 
     * @param db 分库分表
     * @param ids 主键
     */
    public void remove(ShardingDBResource db, List<EntityPK> pks);

}
