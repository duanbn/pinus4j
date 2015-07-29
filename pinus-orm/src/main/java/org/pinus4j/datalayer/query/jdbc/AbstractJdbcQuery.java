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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.datalayer.SlowQueryLogger;
import org.pinus4j.datalayer.query.IDataQuery;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.utils.JdbcUtil;
import org.pinus4j.utils.BeanUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 分库分表查询抽象类. 此类封装了分库分表查询的公共操作. 子类可以针对主库、从库实现相关的查询.
 * 
 * @author duanbn
 */
public abstract class AbstractJdbcQuery implements IDataQuery {

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

    /**
     * 判断一级缓存是否可用
     * 
     * @return true:启用cache, false:不启用
     */
    protected boolean isCacheAvailable(Class<?> clazz, boolean useCache) {
        return primaryCache != null && BeanUtil.isCache(clazz) && useCache;
    }

    /**
     * 判断二级缓存是否可用
     * 
     * @return true:启用cache, false:不启用
     */
    protected boolean isSecondCacheAvailable(Class<?> clazz, boolean useCache) {
        return secondCache != null && BeanUtil.isCache(clazz) && useCache;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // count相关
    // //////////////////////////////////////////////////////////////////////////////////////
    /**
     * 获取全局表的count数
     * 
     * @param conn
     * @param clazz
     * @return count数
     */
    private Number _selectCount(IDBResource dbResource, Class<?> clazz) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectCountGlobalSql(clazz);
            else
                sql = SQLBuilder.buildSelectCountSql(clazz, ((ShardingDBResource) dbResource).getTableIndex());

            ps = conn.prepareStatement(sql);
            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;
            if (constTime > Const.SLOWQUERY_COUNT) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            long count = -1;
            if (rs.next()) {
                count = rs.getLong(1);
            }
            return count;
        } finally {
            JdbcUtil.close(ps, rs);
        }
    }

    /**
     * 带缓存的获取全局表count
     * 
     * @param conn
     * @param clusterName
     * @param clazz
     * @return count数
     * @throws SQLException
     */
    protected Number selectCountWithCache(IDBResource dbResource, Class<?> clazz, boolean useCache) throws SQLException {
        String clusterName = dbResource.getClusterName();
        String tableName = BeanUtil.getTableName(clazz);

        // 操作缓存
        if (isCacheAvailable(clazz, useCache)) {
            long count = 0;
            if (dbResource.isGlobal())
                count = primaryCache.getCountGlobal(clusterName, tableName);
            else
                count = primaryCache.getCount((ShardingDBResource) dbResource);
            if (count > 0) {
                return count;
            }
        }

        long count = 0;
        count = _selectCount(dbResource, clazz).longValue();

        // 操作缓存
        if (isCacheAvailable(clazz, useCache) && count > 0) {
            if (dbResource.isGlobal())
                primaryCache.setCountGlobal(clusterName, tableName, count);
            else
                primaryCache.setCount((ShardingDBResource) dbResource, count);
        }

        return count;
    }

    protected Number selectCountByQuery(IQuery query, IDBResource dbResource, Class<?> clazz) throws SQLException {
        long count = -1;

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectCountByQuery(clazz, -1, query);
            else
                sql = SQLBuilder.buildSelectCountByQuery(clazz, ((ShardingDBResource) dbResource).getTableIndex(),
                        query);

            ps = conn.prepareStatement(sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_COUNT) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            if (rs.next()) {
                count = rs.getLong(1);
            }
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return count;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // findByPk相关
    // //////////////////////////////////////////////////////////////////////////////////////
    private <T> T _selectByPk(IDBResource dbResource, EntityPK pk, Class<T> clazz) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByPk(pk, clazz, -1);
            else
                sql = SQLBuilder.buildSelectByPk(pk, clazz, ((ShardingDBResource) dbResource).getTableIndex());

            ps = conn.prepareStatement(sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_PK) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            List<T> result = SQLBuilder.buildResultObject(clazz, rs);
            if (!result.isEmpty()) {
                return result.get(0);
            }
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return null;
    }

    protected <T> T selectByPkWithCache(IDBResource dbResource, EntityPK pk, Class<T> clazz, boolean useCache)
            throws SQLException {
        String tableName = BeanUtil.getTableName(clazz);
        String clusterName = dbResource.getClusterName();

        T data = null;
        if (isCacheAvailable(clazz, useCache)) {

            if (dbResource.isGlobal())
                data = primaryCache.getGlobal(clusterName, tableName, pk);
            else
                data = primaryCache.get((ShardingDBResource) dbResource, pk);

            if (data == null) {

                data = _selectByPk(dbResource, pk, clazz);

                if (data != null) {
                    if (dbResource.isGlobal())
                        primaryCache.putGlobal(clusterName, tableName, pk, data);
                    else
                        primaryCache.put((ShardingDBResource) dbResource, pk, data);
                }
            }
        } else {
            data = _selectByPk(dbResource, pk, clazz);
        }

        return data;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // findByPks相关
    // //////////////////////////////////////////////////////////////////////////////////////
    private <T> List<T> _selectByPks(IDBResource dbResource, Class<T> clazz, EntityPK[] pks) throws SQLException {
        List<T> result = new ArrayList<T>(1);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByPks(pks, clazz, -1);
            else
                sql = SQLBuilder.buildSelectByPks(pks, clazz, ((ShardingDBResource) dbResource).getTableIndex());

            ps = conn.prepareStatement(sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_PKS) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            result = SQLBuilder.buildResultObject(clazz, rs);
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result;
    }

    private <T> Map<EntityPK, T> _selectByPksWithMap(IDBResource dbResource, Class<T> clazz, EntityPK[] pks)
            throws SQLException {
        Map<EntityPK, T> result = Maps.newHashMap();

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByPks(pks, clazz, -1);
            else
                sql = SQLBuilder.buildSelectByPks(pks, clazz, ((ShardingDBResource) dbResource).getTableIndex());

            ps = conn.prepareStatement(sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_PKS) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            result = SQLBuilder.buildResultObjectAsMap(clazz, rs);
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result;
    }

    protected <T> List<T> selectByPksWithCache(IDBResource dbResource, Class<T> clazz, EntityPK[] pks, boolean useCache)
            throws SQLException {
        List<T> result = new ArrayList<T>();

        if (pks == null || pks.length == 0) {
            return result;
        }

        if (!isCacheAvailable(clazz, useCache)) {
            return _selectByPks(dbResource, clazz, pks);
        }

        String tableName = BeanUtil.getTableName(clazz);
        String clusterName = dbResource.getClusterName();

        List<T> hitResult = null;
        if (dbResource.isGlobal())
            hitResult = primaryCache.getGlobal(clusterName, tableName, pks);
        else
            hitResult = primaryCache.get((ShardingDBResource) dbResource, pks);

        if (hitResult == null || hitResult.isEmpty()) {
            result = _selectByPks(dbResource, clazz, pks);

            if (dbResource.isGlobal())
                primaryCache.putGlobal(clusterName, tableName, result);
            else
                primaryCache.put((ShardingDBResource) dbResource, pks, result);

            return result;
        }

        if (hitResult.size() == pks.length) {
            return hitResult;
        }

        try {
            // 计算没有命中缓存的主键
            Map<EntityPK, T> hitMap = _getPkValues(hitResult);
            List<EntityPK> noHitPkList = Lists.newArrayList();
            for (EntityPK pk : pks) {
                if (hitMap.get(pk) == null) {
                    noHitPkList.add(pk);
                }
            }
            EntityPK[] noHitPks = noHitPkList.toArray(new EntityPK[noHitPkList.size()]);

            // 从数据库中查询没有命中缓存的数据
            Map<EntityPK, T> noHitMap = _selectByPksWithMap(dbResource, clazz, noHitPks);
            if (!noHitMap.isEmpty()) {
                if (dbResource.isGlobal())
                    primaryCache.putGlobal(clusterName, tableName, noHitMap);
                else
                    primaryCache.put((ShardingDBResource) dbResource, noHitMap);
            }

            // 为了保证pks的顺序
            for (EntityPK pk : pks) {
                if (hitMap.get(pk) != null) {
                    result.add(hitMap.get(pk));
                } else {
                    result.add(noHitMap.get(pk));
                }
            }
        } catch (Exception e) {
            result = _selectByPks(dbResource, clazz, pks);
        }

        return result;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // findBySql相关
    // //////////////////////////////////////////////////////////////////////////////////////
    protected List<Map<String, Object>> selectBySql(IDBResource dbResource, SQL sql) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            if (dbResource.isGlobal())
                ps = SQLBuilder.buildSelectBySqlGlobal(conn, sql);
            else
                ps = SQLBuilder.buildSelectBySql(conn, sql, ((ShardingDBResource) dbResource).getTableIndex());

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_SQL) {
                SlowQueryLogger.write(conn, sql, constTime);
            }
            result = (List<Map<String, Object>>) SQLBuilder.buildResultObject(rs);
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // findByQuery相关
    // //////////////////////////////////////////////////////////////////////////////////////
    protected <T> List<T> selectByQuery(IDBResource dbResource, IQuery query, Class<T> clazz) throws SQLException {
        List<T> result = new ArrayList<T>();

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByQuery(clazz, -1, query);
            else
                sql = SQLBuilder.buildSelectByQuery(clazz, ((ShardingDBResource) dbResource).getTableIndex(), query);

            ps = conn.prepareStatement(sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_QUERY) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            result = (List<T>) SQLBuilder.buildResultObject(clazz, rs);
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // getPk相关
    // //////////////////////////////////////////////////////////////////////////////////////
    protected <T> EntityPK[] selectPksByQuery(IDBResource dbResource, IQuery query, Class<T> clazz) throws SQLException {
        List<EntityPK> result = Lists.newArrayList();

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            String sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectPkByQuery(clazz, -1, query);
            else
                sql = SQLBuilder.buildSelectPkByQuery(clazz, ((ShardingDBResource) dbResource).getTableIndex(), query);

            ps = conn.prepareStatement(sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_PKS) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            ResultSetMetaData rsmd = rs.getMetaData();
            int pkNum = rsmd.getColumnCount();
            while (rs.next()) {
                try {
                    PKName[] pkNames = new PKName[pkNum];
                    PKValue[] pkValues = new PKValue[pkNum];
                    for (int i = 1; i <= pkNum; i++) {
                        pkNames[i - 1] = PKName.valueOf(rsmd.getColumnName(i));
                        pkValues[i - 1] = PKValue.valueOf(rs.getObject(i));
                    }
                    result.add(EntityPK.valueOf(pkNames, pkValues));
                } catch (Exception e) {
                    throw new SQLException(e);
                }
            }
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result.toArray(new EntityPK[result.size()]);
    }

    /**
     * 获取列表的主键.
     * 
     * @param entities
     * @return pk值
     * @throws Exception 获取pk值失败
     */
    private <T> Map<EntityPK, T> _getPkValues(List<T> entities) {
        Map<EntityPK, T> map = Maps.newHashMap();

        EntityPK entityPk = null;
        for (T entity : entities) {
            entityPk = BeanUtil.getEntityPK(entity);
            map.put(entityPk, entity);
        }

        return map;
    }

    @Override
    public IDBCluster getDBCluster() {
        return dbCluster;
    }

    @Override
    public void setDBCluster(IDBCluster dbCluster) {
        this.dbCluster = dbCluster;
    }

    @Override
    public void setPrimaryCache(IPrimaryCache primaryCache) {
        this.primaryCache = primaryCache;
    }

    @Override
    public IPrimaryCache getPrimaryCache() {
        return this.primaryCache;
    }

    @Override
    public void setSecondCache(ISecondCache secondCache) {
        this.secondCache = secondCache;
    }

    @Override
    public ISecondCache getSecondCache() {
        return this.secondCache;
    }

    @Override
    public void setTransactionManager(TransactionManager txManager) {
        this.txManager = txManager;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return this.txManager;
    }
}
