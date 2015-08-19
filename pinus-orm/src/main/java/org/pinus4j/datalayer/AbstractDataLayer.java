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

package org.pinus4j.datalayer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import javax.transaction.TransactionManager;

import org.pinus4j.api.SQL;
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;

/**
 * @author duanbn Aug 14, 2015 11:08:17 AM
 */
public abstract class AbstractDataLayer implements IDataLayer {

    /**
     * 数据库集群引用.
     */
    protected IDBCluster         dbCluster;

    /**
     * 一级缓存.
     */
    protected IPrimaryCache      primaryCache;

    /**
     * 二级缓存.
     */
    protected ISecondCache       secondCache;

    protected TransactionManager txManager;

    protected IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();

    @Override
    public IDBCluster getDBCluster() {
        return dbCluster;
    }

    @Override
    public void setDBCluster(IDBCluster dbCluster) {
        this.dbCluster = dbCluster;
    }

    @Override
    public IPrimaryCache getPrimaryCache() {
        return primaryCache;
    }

    @Override
    public void setPrimaryCache(IPrimaryCache primaryCache) {
        this.primaryCache = primaryCache;
    }

    @Override
    public ISecondCache getSecondCache() {
        return secondCache;
    }

    @Override
    public void setSecondCache(ISecondCache secondCache) {
        this.secondCache = secondCache;
    }

    @Override
    public void setTransactionManager(TransactionManager txManager) {
        this.txManager = txManager;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return this.txManager;
    }

    protected void fillParam(PreparedStatement ps, SQL sql) throws SQLException {

        Object val = null;
        for (int i = 1; i <= sql.getParams().size(); i++) {
            val = sql.getParams().get(i - 1);
            if (val instanceof Boolean) {
                if (((Boolean) val).booleanValue()) {
                    ps.setString(i, "1");
                } else {
                    ps.setString(i, "0");
                }
            } else if (val instanceof Character || val instanceof String) {
                ps.setString(i, String.valueOf(val));
            } else if (val instanceof Byte) {
                ps.setByte(i, (Byte) val);
            } else if (val instanceof Short) {
                ps.setShort(i, (Short) val);
            } else if (val instanceof Integer) {
                ps.setInt(i, (Integer) val);
            } else if (val instanceof Long) {
                ps.setLong(i, (Long) val);
            } else if (val instanceof Float) {
                ps.setFloat(i, (Float) val);
            } else if (val instanceof Double) {
                ps.setDouble(i, (Double) val);
            } else if (val instanceof Date) {
                ps.setObject(i, (Date) val);
            } else if (val instanceof Timestamp) {
                ps.setObject(i, (Timestamp) val);
            } else {
                throw new SQLException("无法识别的参数类型" + val);
            }
        }
    }

    /**
     * 判断一级缓存是否可用
     * 
     * @return true:启用cache, false:不启用
     */
    protected boolean isCacheAvailable(Class<?> clazz) {
        return primaryCache != null && entityMetaManager.isCache(clazz);
    }

    /**
     * 判断二级缓存是否可用
     * 
     * @return true:启用cache, false:不启用
     */
    protected boolean isSecondCacheAvailable(Class<?> clazz) {
        return secondCache != null && entityMetaManager.isCache(clazz);
    }

    /**
     * 判断一级缓存是否可用
     * 
     * @return true:启用cache, false:不启用
     */
    protected boolean isCacheAvailable(Class<?> clazz, boolean useCache) {
        return primaryCache != null && entityMetaManager.isCache(clazz) && useCache;
    }

    /**
     * 判断二级缓存是否可用
     * 
     * @return true:启用cache, false:不启用
     */
    protected boolean isSecondCacheAvailable(Class<?> clazz, boolean useCache) {
        return secondCache != null && entityMetaManager.isCache(clazz) && useCache;
    }

}
