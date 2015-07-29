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

package org.pinus4j.datalayer.query.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.datalayer.query.IShardingQuery;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.BeansUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * jdbc sharding query implements.
 *
 * @author duanbn
 * @since 1.1.1
 */
public class ShardingJdbcQueryImpl extends AbstractJdbcQuery implements IShardingQuery {

    public static final Logger LOG = LoggerFactory.getLogger(ShardingJdbcQueryImpl.class);

    @Override
    public Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        List<IDBResource> dbResources = null;
        try {

            tx = txManager.getTransaction();

            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
            }

            long count = 0;
            for (IDBResource dbResource : dbResources) {
                if (tx != null) {
                    tx.enlistResource((ShardingDBResource) dbResource);
                }
                count += selectCountWithCache(dbResource, clazz, useCache).longValue();
            }

            // query from master again
            if (count == 0) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
                for (IDBResource dbResource : dbResources) {
                    if (tx != null) {
                        tx.enlistResource((ShardingDBResource) dbResource);
                    }
                    count += selectCountWithCache((ShardingDBResource) dbResource, clazz, useCache).longValue();
                }
            }

            return count;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResources != null) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
            }
        }
    }

    @Override
    public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
            }

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            long count = selectCountWithCache(dbResource, clazz, useCache).longValue();

            // quer from master again
            if (count == 0) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
                selectCountWithCache(dbResource, clazz, useCache);
            }

            return count;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }
    }

    @Override
    public Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        List<IDBResource> dbResources = null;
        try {
            tx = txManager.getTransaction();

            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
            }

            long count = 0;
            for (IDBResource dbResource : dbResources) {
                if (tx != null) {
                    tx.enlistResource((ShardingDBResource) dbResource);
                }
                count += selectCountByQuery(query, (ShardingDBResource) dbResource, clazz).longValue();
            }

            // query from master again
            if (count == 0) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
                for (IDBResource dbResource : dbResources) {
                    if (tx != null) {
                        tx.enlistResource((ShardingDBResource) dbResource);
                    }
                    count += selectCountByQuery(query, (ShardingDBResource) dbResource, clazz).longValue();
                }
            }

            return count;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResources != null) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
            }
        }
    }

    @Override
    public Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache,
                                  EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        ShardingDBResource dbResource = null;

        try {
            tx = txManager.getTransaction();

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
            }

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            long count = selectCountByQuery(query, dbResource, clazz).longValue();

            // query from master again
            if (count == 0) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
                count = selectCountByQuery(query, dbResource, clazz).longValue();
            }

            return count;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }
    }

    @Override
    public <T> T findByPk(EntityPK pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                          EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {

            tx = txManager.getTransaction();

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
            }

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            List<T> data = selectByPksWithCache(dbResource, clazz, new EntityPK[] { pk }, useCache);

            // query from master again
            if (data == null) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
                data = selectByPksWithCache(dbResource, clazz, new EntityPK[] { pk }, useCache);
            }

            if (data.isEmpty()) {
                return null;
            }

            return data.get(0);
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }
    }

    @Override
    public <T> List<T> findByPkList(List<EntityPK> pkList, Class<T> clazz, boolean useCache,
                                    EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        List<IDBResource> dbResources = null;
        try {

            tx = txManager.getTransaction();

            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
            }

            EntityPK[] entityPkList = pkList.toArray(new EntityPK[pkList.size()]);

            List<T> data = new ArrayList<T>();
            for (IDBResource dbResource : dbResources) {
                if (tx != null) {
                    tx.enlistResource((ShardingDBResource) dbResource);
                }

                data.addAll(selectByPksWithCache((ShardingDBResource) dbResource, clazz, entityPkList, useCache));
            }

            // query from master again
            if (data.isEmpty()) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
                for (IDBResource dbResource : dbResources) {
                    if (tx != null) {
                        tx.enlistResource((ShardingDBResource) dbResource);
                    }

                    data.addAll(selectByPksWithCache((ShardingDBResource) dbResource, clazz, entityPkList, useCache));
                }
            }

            return data;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResources != null) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
            }
        }
    }

    @Override
    public <T> List<T> findByPkList(List<EntityPK> pkList, IShardingKey<?> shardingKey, Class<T> clazz,
                                    boolean useCache, EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {

            tx = txManager.getTransaction();

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
            }

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            EntityPK[] entityPkList = pkList.toArray(new EntityPK[pkList.size()]);

            List<T> data = selectByPksWithCache(dbResource, clazz, entityPkList, useCache);

            if (data.isEmpty()) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
                data = selectByPksWithCache(dbResource, clazz, entityPkList, useCache);
            }

            return data;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }
    }

    @Override
    public <T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        List<T> entities = findByQuery(query, clazz, useCache, masterSlave);

        if (entities.isEmpty()) {
            return null;
        }

        return entities.get(0);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                EnumDBMasterSlave masterSlave) {
        List<T> entities = findByQuery(query, shardingKey, clazz, useCache, masterSlave);

        if (entities.isEmpty()) {
            return null;
        }

        return entities.get(0);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {

        List<IDBResource> dbResources = null;

        try {
            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
            }

            List<T> mergeResult = new ArrayList<T>();
            for (IDBResource dbResource : dbResources) {
                mergeResult.addAll(findByQuery(query, dbResource, clazz, useCache, masterSlave));
            }

            // query from master again
            if (mergeResult.isEmpty()) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
                for (IDBResource dbResource : dbResources) {
                    mergeResult.addAll(findByQuery(query, dbResource, clazz, useCache, masterSlave));
                }
            }

            // FIXME: 如果表是联合主键此处不知道该如何处理
            //            Collections.sort(mergeResult, new Comparator<T>() {
            //                @Override
            //                public int compare(T o1, T o2) {
            //                    long pk1 = ReflectUtil.getPkValue(o1).longValue();
            //                    long pk2 = ReflectUtil.getPkValue(o2).longValue();
            //                    return (int) (pk1 - pk2);
            //                }
            //            });

            List<T> result = null;
            int start = ((DefaultQueryImpl) query).getStart();
            int limit = ((DefaultQueryImpl) query).getLimit();
            if (start > -1 && limit > -1) {
                result = mergeResult.subList(start, start + limit);
            } else if (limit > -1) {
                result = mergeResult.subList(0, limit);
            } else {
                result = mergeResult;
            }

            return result;
        } catch (Exception e) {
            throw new DBOperationException(e);
        }

    }

    @Override
    public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                   EnumDBMasterSlave masterSlave) {

        ShardingDBResource dbResource = null;

        if (EnumDBMasterSlave.MASTER == masterSlave
                || this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
            dbResource = _getDbFromMaster(clazz, shardingKey);
        } else {
            dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
        }

        List<T> data = findByQuery(query, dbResource, clazz, useCache, masterSlave);

        // query from master againe
        if (data.isEmpty()) {
            dbResource = _getDbFromMaster(clazz, shardingKey);
            data = findByQuery(query, dbResource, clazz, useCache, masterSlave);
        }

        return data;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> findByQuery(IQuery query, IDBResource dbResource, Class<T> clazz, boolean useCache,
                                    EnumDBMasterSlave masterSlave) {
        Transaction tx = null;

        try {

            tx = txManager.getTransaction();

            if (tx != null) {
                tx.enlistResource((XAResource) dbResource);
            }

            List<T> result = null;

            if (isSecondCacheAvailable(clazz, useCache)) {
                result = (List<T>) secondCache.get(((DefaultQueryImpl) query).getWhereSql(),
                        (ShardingDBResource) dbResource);
            }

            if (result == null || result.isEmpty()) {
                if (isCacheAvailable(clazz, useCache)) {
                    EntityPK[] entityPks = selectPksByQuery((ShardingDBResource) dbResource, query, clazz);
                    result = selectByPksWithCache(dbResource, clazz, entityPks, useCache);
                } else {
                    result = selectByQuery((ShardingDBResource) dbResource, query, clazz);
                }

                if (isSecondCacheAvailable(clazz, useCache)) {
                    secondCache.put(((DefaultQueryImpl) query).getWhereSql(), (ShardingDBResource) dbResource, result);
                }
            }
            // 过滤从缓存结果, 将没有指定的字段设置为默认值.
            List<T> filteResult = new ArrayList<T>(result.size());
            if (((DefaultQueryImpl) query).hasQueryFields()) {
                for (T obj : result) {
                    try {
                        filteResult.add((T) BeansUtil.cloneWithGivenField(obj, ((DefaultQueryImpl) query).getFields()));
                    } catch (Exception e) {
                        throw new DBOperationException(e);
                    }
                }
                result = filteResult;
            }

            return result;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }
    }

    @Override
    public List<Map<String, Object>> findBySql(SQL sql, EnumDBMasterSlave masterSlave) {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave) {
        ShardingDBResource dbResource = _getDbBySQL(sql, shardingKey, masterSlave);

        Transaction tx = null;
        try {
            tx = txManager.getTransaction();

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            List<Map<String, Object>> result = selectBySql(dbResource, sql);

            // query from master againe
            if (result.isEmpty()) {
                dbResource = _getDbBySQL(sql, shardingKey, EnumDBMasterSlave.MASTER);
                result = selectBySql(dbResource, sql);
            }

            return result;
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }
    }

    private ShardingDBResource _getDbBySQL(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave) {
        ShardingDBResource next = null;
        for (String tableName : sql.getTableNames()) {

            ShardingDBResource cur = null;
            if (EnumDBMasterSlave.MASTER == masterSlave
                    || this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                cur = _getDbFromMaster(tableName, shardingKey);
            } else {
                cur = _getDbFromSlave(tableName, shardingKey, masterSlave);
            }

            if (next != null && (cur != next)) {
                throw new DBOperationException("the tables in sql maybe not at the same database");
            }
            next = cur;
        }

        return next;
    }

    /**
     * 路由选择.
     * 
     * @param clazz 数据对象
     * @param shardingKey 路由因子
     */
    private ShardingDBResource _getDbFromMaster(Class<?> clazz, IShardingKey<?> shardingKey) {
        String tableName = entityMetaManager.getTableName(clazz);
        return _getDbFromMaster(tableName, shardingKey);
    }

    private ShardingDBResource _getDbFromMaster(String tableName, IShardingKey<?> shardingKey) {
        ShardingDBResource shardingDBResource = null;
        try {
            shardingDBResource = (ShardingDBResource) this.dbCluster.selectDBResourceFromMaster(tableName, shardingKey);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[" + shardingDBResource + "]");
            }
        } catch (DBClusterException e) {
            throw new DBOperationException(e);
        }
        return shardingDBResource;
    }

    /**
     * 路由选择.
     * 
     * @param clazz 数据对象
     * @param shardingKey 路由因子
     */
    private ShardingDBResource _getDbFromSlave(Class<?> clazz, IShardingKey<?> shardingKey,
                                               EnumDBMasterSlave masterSlave) {
        String tableName = entityMetaManager.getTableName(clazz);
        ShardingDBResource shardingDBResource = null;
        try {
            shardingDBResource = (ShardingDBResource) this.dbCluster.selectDBResourceFromSlave(tableName, shardingKey,
                    masterSlave);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[" + shardingDBResource + "]");
            }
        } catch (DBClusterException e) {
            throw new DBOperationException(e);
        }
        return shardingDBResource;
    }

    private ShardingDBResource _getDbFromSlave(String tableName, IShardingKey<?> shardingKey, EnumDBMasterSlave slave) {
        ShardingDBResource shardingDBResource = null;
        try {
            shardingDBResource = (ShardingDBResource) this.dbCluster.selectDBResourceFromSlave(tableName, shardingKey,
                    slave);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[" + shardingDBResource + "]");
            }
        } catch (DBClusterException e) {
            throw new DBOperationException(e);
        }
        return shardingDBResource;
    }

}
