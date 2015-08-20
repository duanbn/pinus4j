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
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.BeansUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * global query implements.
 *
 * @author duanbn.
 * @since 1.1.1
 */
public class GlobalJdbcQueryImpl extends AbstractJdbcQuery implements IGlobalQuery {

    public static final Logger LOG = LoggerFactory.getLogger(GlobalJdbcQueryImpl.class);

    @Override
    public Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        String clusterName = entityMetaManager.getClusterName(clazz);
        String tableName = entityMetaManager.getTableName(clazz);

        Transaction tx = null;
        IDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            boolean isFromSlave = false;

            // select db resource.
            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
            } else {
                dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
                isFromSlave = true;
            }

            if (tx != null) {
                tx.enlistResource((XAResource) dbResource);
            }

            long count = selectCountWithCache(dbResource, clazz, useCache).longValue();
            if (count == 0 && isFromSlave) {
                dbResource.close();
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);

                if (tx != null) {
                    tx.enlistResource((XAResource) dbResource);
                }

                count = selectCountWithCache(dbResource, clazz, useCache).longValue();
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
        String clusterName = entityMetaManager.getClusterName(clazz);
        String tableName = entityMetaManager.getTableName(clazz);

        Transaction tx = null;
        IDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
            } else {
                dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
                isFromSlave = true;
            }

            if (tx != null) {
                tx.enlistResource((XAResource) dbResource);
            }

            long count = selectCountByQuery(query, dbResource, clazz).longValue();
            if (count == 0 && isFromSlave) {
                dbResource.close();
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);

                if (tx != null) {
                    tx.enlistResource((XAResource) dbResource);
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
    public <T> T findByPk(EntityPK pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        List<T> result = findByPkList(Lists.newArrayList(pk), clazz, useCache, masterSlave);

        if (!result.isEmpty()) {
            return result.get(0);
        }

        return null;
    }

    @Override
    public <T> List<T> findByPkList(List<EntityPK> pkList, Class<T> clazz, boolean useCache,
                                    EnumDBMasterSlave masterSlave) {
        List<T> result = Lists.newArrayList();

        String clusterName = entityMetaManager.getClusterName(clazz);
        String tableName = entityMetaManager.getTableName(clazz);

        Transaction tx = null;
        IDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
            } else {
                dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
                isFromSlave = true;
            }
            if (tx != null) {
                tx.enlistResource((XAResource) dbResource);
            }

            EntityPK[] entityPks = pkList.toArray(new EntityPK[pkList.size()]);
            Map<EntityPK, T> data = selectByPksWithCache(dbResource, clazz, entityPks, null, useCache);
            if (data.isEmpty() && isFromSlave) {
                dbResource.close();
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);

                if (tx != null) {
                    tx.enlistResource((XAResource) dbResource);
                }

                data = selectByPksWithCache(dbResource, clazz, entityPks, null, useCache);
            }

            result.addAll(data.values());
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> findByQuery(IQuery<T> query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        List<T> result = Lists.newArrayList();

        if (query == null) {
            query = new DefaultQueryImpl<T>();
        }

        String clusterName = entityMetaManager.getClusterName(clazz);
        String tableName = entityMetaManager.getTableName(clazz);

        if (isSecondCacheAvailable(clazz, useCache)) {
            List<T> sCacheData = secondCache.getGlobal(((DefaultQueryImpl<T>) query).getWhereSql().getSql(),
                    clusterName, tableName);
            if (sCacheData != null) {
                return sCacheData;
            }
        }

        Transaction tx = null;
        IDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            boolean isFromSlave = false;

            if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
                dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
            } else {
                dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
                isFromSlave = true;
            }

            if (tx != null) {
                tx.enlistResource((XAResource) dbResource);
            }

            if (isCacheAvailable(clazz, useCache)) {
                EntityPK[] entityPks = selectPksByQuery(dbResource, query, clazz);
                Map<EntityPK, T> datas = selectByPksWithCache(dbResource, clazz, entityPks,
                        ((DefaultQueryImpl<T>) query).getOrderList(), useCache);

                if ((datas == null || datas.isEmpty()) && isFromSlave) {
                    dbResource.close();
                    dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);

                    if (tx != null) {
                        tx.enlistResource((XAResource) dbResource);
                    }

                    datas = selectByPksWithCache(dbResource, clazz, entityPks,
                            ((DefaultQueryImpl<T>) query).getOrderList(), useCache);
                }

                result.addAll(datas.values());
            } else {
                result = selectByQuery(dbResource, query, clazz);
                if ((result == null || result.isEmpty()) && isFromSlave) {
                    dbResource.close();
                    dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);

                    if (tx != null) {
                        tx.enlistResource((XAResource) dbResource);
                    }

                    result = selectByQuery(dbResource, query, clazz);
                }
            }

            if (isSecondCacheAvailable(clazz, useCache)) {
                secondCache.putGlobal(((DefaultQueryImpl<T>) query).getWhereSql().getSql(), clusterName, tableName,
                        result);
            }

            // 过滤从缓存结果, 将没有指定的字段设置为默认值.
            List<T> filteResult = new ArrayList<T>(result.size());
            if (((DefaultQueryImpl<T>) query).hasQueryFields()) {
                for (T obj : result) {
                    filteResult.add((T) BeansUtil.cloneWithGivenField(obj, ((DefaultQueryImpl<T>) query).getFields()));
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
    public List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz, EnumDBMasterSlave masterSlave) {
        String clusterName = entityMetaManager.getClusterName(clazz);

        IDBResource next = null;

        for (String tableName : sql.getTableNames()) {
            IDBResource cur = null;

            try {
                if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
                    cur = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
                } else {
                    cur = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
                }
            } catch (DBClusterException e) {
                throw new DBOperationException(e);
            }

            if (next != null && (cur != next)) {
                throw new DBOperationException("the tables in sql maybe not at the same database");
            }

            next = cur;
        }

        Transaction tx = null;
        try {

            tx = txManager.getTransaction();

            if (tx != null) {
                tx.enlistResource((XAResource) next);
            }

            List<Map<String, Object>> result = selectBySql(next, sql);

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
            if (tx == null && next != null) {
                next.close();
            }
        }
    }

}
