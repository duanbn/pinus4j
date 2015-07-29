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

package org.pinus4j.datalayer.update.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.Transaction;

import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.datalayer.update.IShardingUpdate;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.BeanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 分库分表更新实现. 更新操作包括插入、删除、更新，这些操作只操作主库.
 * 
 * @author duanbn
 */
public class ShardingJdbcUpdateImpl extends AbstractJdbcUpdate implements IShardingUpdate {

    /**
     * 日志.
     */
    public static final Logger LOG = LoggerFactory.getLogger(ShardingJdbcUpdateImpl.class);

    @SuppressWarnings({ "rawtypes" })
    @Override
    public PKValue save(Object entity, IShardingKey shardingKey) {
        Class clazz = entity.getClass();
        String tableName = BeanUtil.getTableName(clazz);

        PKValue pk = null;
        List<Object> entities = new ArrayList<Object>(1);
        entities.add(entity);

        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            dbResource = _getDbFromMaster(tableName, shardingKey);
            Connection conn = dbResource.getConnection();

            List<PKValue> genPks = _saveBatch(conn, entities, dbResource.getTableIndex());
            if (!genPks.isEmpty())
                pk = genPks.get(0);

            if (tx != null) {
                tx.enlistResource(dbResource);
            } else {
                dbResource.commit();
            }

            if (isCacheAvailable(clazz)) {
                primaryCache.incrCount(dbResource, 1);
            }
            if (isSecondCacheAvailable(clazz)) {
                secondCache.remove(dbResource);
            }
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            } else {
                if (dbResource != null) {
                    dbResource.rollback();
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }

        return pk;
    }

    @Override
    public PKValue[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
        Class<?> clazz = entities.get(0).getClass();
        String tableName = BeanUtil.getTableName(clazz);

        List<PKValue> pks = Lists.newArrayList();

        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            dbResource = _getDbFromMaster(tableName, shardingKey);
            Connection conn = dbResource.getConnection();

            List<PKValue> genPks = _saveBatch(conn, entities, dbResource.getTableIndex());
            pks.addAll(genPks);

            if (tx != null) {
                tx.enlistResource(dbResource);
            } else {
                dbResource.commit();
            }

            if (isCacheAvailable(clazz)) {
                primaryCache.incrCount(dbResource, pks.size());
            }
            if (isSecondCacheAvailable(clazz)) {
                secondCache.remove(dbResource);
            }
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            } else {
                if (dbResource != null) {
                    dbResource.rollback();
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }

        return pks.toArray(new PKValue[pks.size()]);
    }

    @Override
    public void update(Object entity, IShardingKey<?> shardingKey) {
        List<Object> entities = new ArrayList<Object>();
        entities.add(entity);
        updateBatch(entities, shardingKey);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
        Class<?> clazz = entities.get(0).getClass();

        String talbeName = BeanUtil.getTableName(clazz);

        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            dbResource = _getDbFromMaster(talbeName, shardingKey);
            Connection conn = dbResource.getConnection();

            _updateBatch(conn, entities, dbResource.getTableIndex());

            if (tx != null) {
                tx.enlistResource(dbResource);
            } else {
                dbResource.commit();
            }

            // 清理缓存
            if (isCacheAvailable(clazz)) {
                List<EntityPK> entityPkList = Lists.newArrayList();
                for (Object entity : entities) {
                    entityPkList.add(BeanUtil.getEntityPK(entity));
                }
                primaryCache.remove(dbResource, entityPkList);
            }
            if (isSecondCacheAvailable(clazz)) {
                secondCache.remove(dbResource);
            }
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            } else {
                if (dbResource != null) {
                    dbResource.rollback();
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
    public void removeByPk(EntityPK pk, IShardingKey<?> shardingKey, Class<?> clazz) {
        List<EntityPK> pks = Lists.newArrayListWithCapacity(1);
        pks.add(pk);
        removeByPks(pks, shardingKey, clazz);
    }

    @Override
    public void removeByPks(List<EntityPK> pks, IShardingKey<?> shardingKey, Class<?> clazz) {
        String talbeName = BeanUtil.getTableName(clazz);

        Transaction tx = null;
        ShardingDBResource dbResource = null;
        try {
            tx = txManager.getTransaction();
            dbResource = _getDbFromMaster(talbeName, shardingKey);

            Connection conn = dbResource.getConnection();

            _removeByPks(conn, pks, clazz, dbResource.getTableIndex());

            if (tx != null) {
                tx.enlistResource(dbResource);
            } else {
                dbResource.commit();
            }

            // 删除缓存
            if (isCacheAvailable(clazz)) {
                primaryCache.remove(dbResource, pks);
                primaryCache.decrCount(dbResource, pks.size());
            }
            if (isSecondCacheAvailable(clazz)) {
                secondCache.remove(dbResource);
            }
        } catch (Exception e) {
            if (tx != null) {
                try {
                    tx.rollback();
                } catch (Exception e1) {
                    throw new DBOperationException(e1);
                }
            } else {
                if (dbResource != null) {
                    dbResource.rollback();
                }
            }

            throw new DBOperationException(e);
        } finally {
            if (tx == null && dbResource != null) {
                dbResource.close();
            }
        }

    }

    /**
     * 路由选择.
     * 
     * @param clazz 数据对象
     * @param shardingKey 路由因子
     */
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

}
