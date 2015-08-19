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

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.AbstractDataLayer;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.datalayer.SlowQueryLogger;
import org.pinus4j.datalayer.query.IDataQuery;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.utils.JdbcUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 分库分表查询抽象类. 此类封装了分库分表查询的公共操作. 子类可以针对主库、从库实现相关的查询.
 * 
 * @author duanbn
 */
public abstract class AbstractJdbcQuery extends AbstractDataLayer implements IDataQuery {

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
        String tableName = entityMetaManager.getTableName(clazz);

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

    protected <T> Number selectCountByQuery(IQuery<T> query, IDBResource dbResource, Class<T> clazz)
            throws SQLException {
        long count = -1;

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            SQL sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectCountByQuery(clazz, -1, query);
            else
                sql = SQLBuilder.buildSelectCountByQuery(clazz, ((ShardingDBResource) dbResource).getTableIndex(),
                        query);

            ps = conn.prepareStatement(sql.getSql());
            fillParam(ps, sql);

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
    // findByPks相关
    // //////////////////////////////////////////////////////////////////////////////////////
    private <T> List<T> _selectByPks(IDBResource dbResource, Class<T> clazz, EntityPK[] pks) throws SQLException {
        List<T> result = new ArrayList<T>(1);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            SQL sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByPks(pks, clazz, -1);
            else
                sql = SQLBuilder.buildSelectByPks(pks, clazz, ((ShardingDBResource) dbResource).getTableIndex());

            ps = conn.prepareStatement(sql.getSql());
            fillParam(ps, sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_PKS) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            result = SQLBuilder.createResultObject(clazz, rs);
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

            SQL sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByPks(pks, clazz, -1);
            else
                sql = SQLBuilder.buildSelectByPks(pks, clazz, ((ShardingDBResource) dbResource).getTableIndex());

            ps = conn.prepareStatement(sql.getSql());
            fillParam(ps, sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_PKS) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            result = SQLBuilder.createResultObjectAsMap(clazz, rs);
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

        String tableName = entityMetaManager.getTableName(clazz);
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
            result = (List<Map<String, Object>>) SQLBuilder.createResultObject(rs);
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // findByQuery相关
    // //////////////////////////////////////////////////////////////////////////////////////
    protected <T> List<T> selectByQuery(IDBResource dbResource, IQuery<T> query, Class<T> clazz) throws SQLException {
        List<T> result = null;

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            SQL sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectByQuery(clazz, -1, query);
            else
                sql = SQLBuilder.buildSelectByQuery(clazz, ((ShardingDBResource) dbResource).getTableIndex(), query);

            ps = conn.prepareStatement(sql.getSql());
            fillParam(ps, sql);

            long begin = System.currentTimeMillis();
            rs = ps.executeQuery();
            long constTime = System.currentTimeMillis() - begin;

            if (constTime > Const.SLOWQUERY_QUERY) {
                SlowQueryLogger.write(conn, sql, constTime);
            }

            long start = System.currentTimeMillis();
            result = (List<T>) SQLBuilder.createResultObject(clazz, rs);
            System.out.println("const " + (System.currentTimeMillis() - start) + "ms");
        } finally {
            JdbcUtil.close(ps, rs);
        }

        return result;
    }

    // //////////////////////////////////////////////////////////////////////////////////////
    // getPk相关
    // //////////////////////////////////////////////////////////////////////////////////////
    protected <T> EntityPK[] selectPksByQuery(IDBResource dbResource, IQuery<T> query, Class<T> clazz)
            throws SQLException {
        List<EntityPK> result = Lists.newArrayList();

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Connection conn = dbResource.getConnection();

            SQL sql = null;
            if (dbResource.isGlobal())
                sql = SQLBuilder.buildSelectPkByQuery(clazz, -1, query);
            else
                sql = SQLBuilder.buildSelectPkByQuery(clazz, ((ShardingDBResource) dbResource).getTableIndex(), query);

            ps = conn.prepareStatement(sql.getSql());
            fillParam(ps, sql);

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
            entityPk = entityMetaManager.getEntityPK(entity);
            map.put(entityPk, entity);
        }

        return map;
    }

}
