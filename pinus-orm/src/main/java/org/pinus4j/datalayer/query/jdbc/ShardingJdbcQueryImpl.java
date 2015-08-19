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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.api.query.impl.Order;
import org.pinus4j.api.query.impl.DefaultQueryImpl.OrderBy;
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
            boolean isFromSlave = false;

            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
                isFromSlave = true;
            }

            long count = 0;
            for (IDBResource dbResource : dbResources) {
                if (tx != null) {
                    tx.enlistResource((ShardingDBResource) dbResource);
                }
                count += selectCountWithCache(dbResource, clazz, useCache).longValue();
            }

            // query from master again
            if (count == 0 && isFromSlave) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
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
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || !this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
                isFromSlave = true;
            }
            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            long count = selectCountWithCache(dbResource, clazz, useCache).longValue();

            // quer from master again
            if (count == 0 && isFromSlave) {
                dbResource.close();
                dbResource = _getDbFromMaster(clazz, shardingKey);
                if (tx != null) {
                    tx.enlistResource(dbResource);
                }
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
    public <T> Number getCountByQuery(IQuery<T> query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        List<IDBResource> dbResources = null;
        try {
            tx = txManager.getTransaction();
            boolean isFromSlave = false;

            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
                isFromSlave = true;
            }

            long count = 0;
            for (IDBResource dbResource : dbResources) {
                if (tx != null) {
                    tx.enlistResource((ShardingDBResource) dbResource);
                }
                count += selectCountByQuery(query, (ShardingDBResource) dbResource, clazz).longValue();
            }

            // query from master again
            if (count == 0 && isFromSlave) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
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
    public <T> Number getCountByQuery(IQuery<T> query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                      EnumDBMasterSlave masterSlave) {
        Transaction tx = null;
        ShardingDBResource dbResource = null;

        try {
            tx = txManager.getTransaction();
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || !this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
                isFromSlave = true;
            }
            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            long count = selectCountByQuery(query, dbResource, clazz).longValue();

            // query from master again
            if (count == 0 && isFromSlave) {
                dbResource.close();
                dbResource = _getDbFromMaster(clazz, shardingKey);
                if (tx != null) {
                    tx.enlistResource(dbResource);
                }
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
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || !this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
                isFromSlave = true;
            }
            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            List<T> data = selectByPksWithCache(dbResource, clazz, new EntityPK[] { pk }, useCache);

            // query from master again
            if (data == null && isFromSlave) {
                dbResource.close();
                dbResource = _getDbFromMaster(clazz, shardingKey);
                if (tx != null) {
                    tx.enlistResource(dbResource);
                }
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
            boolean isFromSlave = false;

            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
                isFromSlave = true;
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
            if (data.isEmpty() && isFromSlave) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
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
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave
                    || !this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                dbResource = _getDbFromMaster(clazz, shardingKey);
            } else {
                dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
                isFromSlave = true;
            }

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            EntityPK[] entityPkList = pkList.toArray(new EntityPK[pkList.size()]);

            List<T> data = selectByPksWithCache(dbResource, clazz, entityPkList, useCache);

            if (data.isEmpty() && isFromSlave) {
                dbResource.close();
                dbResource = _getDbFromMaster(clazz, shardingKey);
                if (tx != null) {
                    tx.enlistResource(dbResource);
                }
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
    public <T> T findOneByQuery(IQuery<T> query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        List<T> entities = findByQuery(query, clazz, useCache, masterSlave);

        if (entities.isEmpty()) {
            return null;
        }

        return entities.get(0);
    }

    @Override
    public <T> T findOneByQuery(IQuery<T> query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                EnumDBMasterSlave masterSlave) {
        List<T> entities = findByQuery(query, shardingKey, clazz, useCache, masterSlave);

        if (entities.isEmpty()) {
            return null;
        }

        return entities.get(0);
    }

    @Override
    public <T> List<T> findByQuery(IQuery<T> query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {

        boolean isFromSlave = false;
        List<IDBResource> dbResources = null;
        DefaultQueryImpl<T> internalQuery = (DefaultQueryImpl<T>) query;

        try {
            String clusterName = entityMetaManager.getClusterName(clazz);
            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isShardingSlaveExist(clusterName)) {
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
            } else {
                dbResources = this.dbCluster.getAllSlaveShardingDBResource(clazz, masterSlave);
                isFromSlave = true;
            }

            int start = internalQuery.getStart();
            int limit = internalQuery.getLimit();
            int sum = start + limit;
            if (start >= 0 && limit > 0)
                internalQuery.limit(0, sum);

            final List<OrderBy> orderList = internalQuery.getOrderList();

            boolean isOrderQuery = false;
            if (orderList != null && !orderList.isEmpty()) {
                isOrderQuery = true;
            }

            List<T> mergeResult = new ArrayList<T>();
            for (IDBResource dbResource : dbResources) {
                if (isOrderQuery) {
                    mergeResult.addAll(findByQuery(internalQuery, dbResource, clazz, useCache, masterSlave));
                } else {
                    mergeResult.addAll(findByQuery(internalQuery, dbResource, clazz, useCache, masterSlave));
                    if (mergeResult.size() >= sum) {
                        break;
                    }
                }
            }

            // query from master again
            if (mergeResult.isEmpty() && isFromSlave) {
                for (IDBResource dbResource : dbResources) {
                    dbResource.close();
                }
                dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
                for (IDBResource dbResource : dbResources) {
                    if (isOrderQuery) {
                        mergeResult.addAll(findByQuery(internalQuery, dbResource, clazz, useCache, masterSlave));
                    } else {
                        mergeResult.addAll(findByQuery(internalQuery, dbResource, clazz, useCache, masterSlave));
                        if (mergeResult.size() >= sum) {
                            break;
                        }
                    }
                }
            }

            // if order by exists, sort by order.
            if (orderList != null && !orderList.isEmpty()) {
                Collections.sort(mergeResult, new Comparator<T>() {

                    @Override
                    public int compare(T o1, T o2) {
                        Object v1 = null, v2 = null;
                        Class<?> fieldType = null;
                        int compareVal = 0;

                        for (OrderBy order : orderList) {
                            v1 = BeansUtil.getProperty(o1, order.getField());
                            v2 = BeansUtil.getProperty(o2, order.getField());
                            fieldType = order.getFieldType();

                            if (fieldType == Boolean.class || fieldType == Boolean.TYPE) {
                                compareVal = ((Boolean) v1).compareTo((Boolean) v2);
                            } else if (fieldType == Character.class || fieldType == Character.TYPE) {
                                compareVal = ((Character) v1).compareTo((Character) v2);
                            } else if (fieldType == Byte.class || fieldType == Byte.TYPE) {
                                compareVal = ((Byte) v1).compareTo((Byte) v2);
                            } else if (fieldType == Short.class || fieldType == Short.TYPE) {
                                compareVal = ((Short) v1).compareTo((Short) v2);
                            } else if (fieldType == Integer.class || fieldType == Integer.TYPE) {
                                compareVal = ((Integer) v1).compareTo((Integer) v2);
                            } else if (fieldType == Long.class || fieldType == Long.TYPE) {
                                compareVal = ((Long) v1).compareTo((Long) v2);
                            } else if (fieldType == Float.class || fieldType == Float.TYPE) {
                                compareVal = ((Float) v1).compareTo((Float) v2);
                            } else if (fieldType == Double.class || fieldType == Double.TYPE) {
                                compareVal = ((Double) v1).compareTo((Double) v2);
                            } else if (fieldType == String.class) {
                                compareVal = ((String) v1).compareTo((String) v2);
                            } else if (fieldType == Date.class) {
                                compareVal = ((Date) v1).compareTo((Date) v2);
                            } else if (fieldType == Timestamp.class) {
                                compareVal = ((Timestamp) v1).compareTo((Timestamp) v2);
                            } else {
                                throw new RuntimeException("无法排序的类型" + order);
                            }

                            if (order.getOrder() == Order.DESC) {
                                compareVal *= -1;
                            }

                            if (compareVal != 0) {
                                break;
                            }
                        }

                        return compareVal;
                    }

                });
            }

            // get result
            List<T> result = null;
            if (start > -1 && limit > -1) {
                int fromIndex = start;
                int endIndex = sum > mergeResult.size() ? mergeResult.size() : sum;
                result = mergeResult.subList(fromIndex, endIndex);
            } else if (limit > -1) {
                result = mergeResult.subList(0, limit - 1);
            } else {
                result = mergeResult;
            }

            return result;
        } catch (Exception e) {
            throw new DBOperationException(e);
        }

    }

    @Override
    public <T> List<T> findByQuery(IQuery<T> query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                   EnumDBMasterSlave masterSlave) {

        boolean isFromSlave = false;
        ShardingDBResource dbResource = null;

        if (EnumDBMasterSlave.MASTER == masterSlave
                || !this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
            dbResource = _getDbFromMaster(clazz, shardingKey);
        } else {
            dbResource = _getDbFromSlave(clazz, shardingKey, masterSlave);
            isFromSlave = true;
        }

        List<T> data = findByQuery(query, dbResource, clazz, useCache, masterSlave);

        // query from master againe
        if (data.isEmpty() && isFromSlave) {
            dbResource.close();
            dbResource = _getDbFromMaster(clazz, shardingKey);
            data = findByQuery(query, dbResource, clazz, useCache, masterSlave);
        }

        return data;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> findByQuery(IQuery<T> query, IDBResource dbResource, Class<T> clazz, boolean useCache,
                                    EnumDBMasterSlave masterSlave) {
        Transaction tx = null;

        try {

            tx = txManager.getTransaction();

            if (tx != null) {
                tx.enlistResource((XAResource) dbResource);
            }

            List<T> result = null;

            if (isSecondCacheAvailable(clazz, useCache)) {
                result = (List<T>) secondCache.get(((DefaultQueryImpl<T>) query).getWhereSql().getSql(),
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
                    secondCache.put(((DefaultQueryImpl<T>) query).getWhereSql().getSql(), (ShardingDBResource) dbResource,
                            result);
                }
            }
            // 过滤从缓存结果, 将没有指定的字段设置为默认值.
            List<T> filteResult = new ArrayList<T>(result.size());
            if (((DefaultQueryImpl<T>) query).hasQueryFields()) {
                for (T obj : result) {
                    try {
                        filteResult.add((T) BeansUtil.cloneWithGivenField(obj,
                                ((DefaultQueryImpl<T>) query).getFields()));
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
        Transaction tx = null;
        ShardingDBResource dbResource = _getDbBySQL(sql, shardingKey, masterSlave);
        try {
            tx = txManager.getTransaction();

            if (tx != null) {
                tx.enlistResource(dbResource);
            }

            boolean isFromSlave = false;
            if (EnumDBMasterSlave.MASTER != masterSlave
                    && this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
                isFromSlave = true;
            }

            List<Map<String, Object>> result = selectBySql(dbResource, sql);

            // query from master againe
            if (result.isEmpty() && isFromSlave) {
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
                    || !this.dbCluster.isShardingSlaveExist(shardingKey.getClusterName())) {
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
