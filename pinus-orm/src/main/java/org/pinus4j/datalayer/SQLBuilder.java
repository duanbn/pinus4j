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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.constant.Const;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * SQL工具类.
 * 
 * @author duanbn
 */
public class SQLBuilder {

    public static final Logger               LOG               = LoggerFactory.getLogger(SQLBuilder.class);

    /**
     * select count语句缓存.
     */
    private static final Map<String, String> _selectCountCache = new ConcurrentHashMap<String, String>();

    private static final SimpleDateFormat    sdf               = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static IEntityMetaManager        entityMetaManager = DefaultEntityMetaManager.getInstance();

    /**
     * 拼装sql. SELECT pkName FROM tableName {IQuery.getSql()}
     * 
     * @return sql语句.
     */
    public static String buildSelectPkByQuery(Class<?> clazz, int tableIndex, IQuery query) {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        PKName[] pkNames = entityMetaManager.getPkName(clazz);
        StringBuilder pkField = new StringBuilder();
        for (PKName pkName : pkNames) {
            pkField.append(pkName.getValue()).append(',');
        }
        pkField.deleteCharAt(pkField.length() - 1);

        StringBuilder SQL = new StringBuilder("SELECT " + pkField.toString() + " FROM ");
        SQL.append(tableName);
        String whereSql = ((DefaultQueryImpl) query).getWhereSql();
        if (StringUtil.isNotBlank(whereSql))
            SQL.append(((DefaultQueryImpl) query).getWhereSql());

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 拼装sql. SELECT {fields} FROM tableName {IQuery.getSql()}
     * 
     * @return sql语句.
     */
    public static String buildSelectByQuery(Class<?> clazz, int tableIndex, IQuery query) {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder fields = new StringBuilder();
        if (((DefaultQueryImpl) query).hasQueryFields()) {
            for (String field : ((DefaultQueryImpl) query).getFields()) {
                fields.append(field).append(",");
            }
            fields.deleteCharAt(fields.length() - 1);
        } else {
            fields.append("*");
        }

        StringBuilder SQL = new StringBuilder("SELECT ");
        SQL.append(fields.toString()).append(" FROM ");
        SQL.append(tableName);
        String whereSql = ((DefaultQueryImpl) query).getWhereSql();
        if (StringUtil.isNotBlank(whereSql))
            SQL.append(((DefaultQueryImpl) query).getWhereSql());

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    public static String buildSelectCountByQuery(Class<?> clazz, int tableIndex, IQuery query) {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);
        StringBuilder SQL = new StringBuilder("SELECT count(*) FROM ");
        SQL.append(tableName);

        if (query != null) {
            String whereSql = ((DefaultQueryImpl) query).getWhereSql();
            if (StringUtil.isNotBlank(whereSql))
                SQL.append(((DefaultQueryImpl) query).getWhereSql());
        }

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    public static PreparedStatement buildSelectBySqlGlobal(Connection conn, SQL sql) throws SQLException {
        debugSQL(sql.toString());

        PreparedStatement ps = conn.prepareStatement(sql.getSql());
        Object[] params = sql.getParams();
        if (params != null) {
            for (int i = 1; i <= params.length; i++) {
                ps.setObject(i, params[i - 1]);
            }
        }
        return ps;
    }

    /**
     * 拼装sql. 根据SQL对象生成查询语句, 此sql语句不能包含limit
     * 
     * @param conn 数据库连接
     * @param sql 查询对象
     * @param tableIndex 分表下标
     * @return PreparedStatement
     * @throws SQLException
     */
    public static PreparedStatement buildSelectBySql(Connection conn, SQL sql, int tableIndex) throws SQLException {
        String s = SQLParser.addTableIndex(sql.getSql(), tableIndex);

        debugSQL(sql.toString());

        PreparedStatement ps = conn.prepareStatement(s);
        Object[] params = sql.getParams();
        if (params != null) {
            for (int i = 1; i <= params.length; i++) {
                ps.setObject(i, params[i - 1]);
            }
        }
        return ps;
    }

    public static String buildSelectCountGlobalSql(Class<?> clazz) {
        return buildSelectCountGlobalSql(clazz, null);
    }

    public static String buildSelectCountGlobalSql(Class<?> clazz, IQuery query) {
        String tableName = entityMetaManager.getTableName(clazz, -1);

        StringBuilder SQL = new StringBuilder("SELECT count(*) ").append("FROM ");
        SQL.append(tableName);
        if (query != null) {
            SQL.append(((DefaultQueryImpl) query).getWhereSql());
        }
        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 拼装sql. SELECT count(*) FROM tableName
     * 
     * @param clazz 数据对象class
     * @param tableIndex 分表下标
     * @return SELECT count(*) FROM tableName
     */
    public static String buildSelectCountSql(Class<?> clazz, int tableIndex) {
        String sql = _selectCountCache.get(clazz.getName() + tableIndex);
        if (sql != null) {
            debugSQL(sql);
            return sql;
        }

        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder SQL = new StringBuilder("SELECT count(*) ").append("FROM ");
        SQL.append(tableName);
        debugSQL(SQL.toString());

        _selectCountCache.put(clazz.getName() + tableIndex, SQL.toString());

        return SQL.toString();
    }

    /**
     * 给定数据库查询结果集创建数据对性.
     * 
     * @param rs 数据库查询结果集
     * @return 数据对象列表
     */
    public static List<Map<String, Object>> buildResultObject(ResultSet rs) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        ResultSetMetaData rsmd = rs.getMetaData();
        Map<String, Object> one = null;
        while (rs.next()) {
            try {
                one = new HashMap<String, Object>();
                String fieldName = null;
                Object value = null;
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    fieldName = rsmd.getColumnName(i);
                    value = rs.getObject(i);
                    one.put(fieldName, value);
                }
                list.add(one);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        return list;
    }

    /**
     * 给定数据库查询结果集创建数据对性.
     * 
     * @param clazz 数据对象class
     * @param rs 数据库查询结果集
     * @return 数据对象列表
     */
    public static <T> List<T> buildResultObject(Class<T> clazz, ResultSet rs) throws SQLException {
        List<T> list = new ArrayList<T>();

        ResultSetMetaData rsmd = rs.getMetaData();
        T one = null;
        while (rs.next()) {
            try {
                one = (T) clazz.newInstance();
                String fieldName = null;
                Field f = null;
                Object value = null;
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    fieldName = rsmd.getColumnName(i);
                    f = BeansUtil.getField(clazz, fieldName);
                    value = _getRsValue(rs, f, i);
                    BeansUtil.setProperty(one, fieldName, value);
                }
                list.add(one);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        return list;
    }

    /**
     * 将数据转换为数据对象
     * 
     * @param clazz 数据对象
     * @param rs 结果集
     * @return {pkValue, Object}
     * @throws SQLException
     */
    public static <T> Map<EntityPK, T> buildResultObjectAsMap(Class<T> clazz, ResultSet rs) throws SQLException {
        Map<EntityPK, T> map = Maps.newHashMap();

        ResultSetMetaData rsmd = rs.getMetaData();
        T one = null;
        String fieldName = null;
        PKName[] pkNames = entityMetaManager.getPkName(clazz);
        PKValue[] pkValues = null;

        Field f = null;
        Object value = null;
        while (rs.next()) {
            try {
                one = (T) clazz.newInstance();

                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    fieldName = rsmd.getColumnName(i);
                    f = BeansUtil.getField(clazz, fieldName);
                    value = _getRsValue(rs, f, i);
                    BeansUtil.setProperty(one, fieldName, value);
                }

                pkValues = new PKValue[pkNames.length];
                for (int i = 0; i < pkNames.length; i++) {
                    pkValues[i] = PKValue.valueOf(rs.getObject(pkNames[i].getValue()));
                }

                map.put(EntityPK.valueOf(pkNames, pkValues), one);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        return map;
    }

    private static Object _getRsValue(ResultSet rs, Field f, int i) throws SQLException {
        Object value = rs.getObject(i);

        if (value != null) {
            if (f.getType() == Boolean.TYPE || f.getType() == Boolean.class) {
                value = rs.getString(i).equals(Const.TRUE) ? true : false;
            } else if (f.getType() == Byte.TYPE || f.getType() == Byte.class) {
                value = rs.getByte(i);
            } else if (f.getType() == Character.TYPE || f.getType() == Character.class) {
                String s = rs.getString(i);
                if (s.length() > 0)
                    value = rs.getString(i).charAt(0);
                else
                    value = new Character('\u0000');
            } else if (f.getType() == Short.TYPE || f.getType() == Short.class) {
                value = rs.getShort(i);
            } else if (f.getType() == Long.TYPE || f.getType() == Long.class) {
                value = rs.getLong(i);
            } else if (f.getType() == Integer.TYPE || f.getType() == Integer.class) {
                if (value instanceof Boolean) {
                    if ((Boolean) value) {
                        value = new Integer(1);
                    } else {
                        value = new Integer(0);
                    }
                }
            }
        }

        return value;
    }

    /**
     * 拼装select sql. SELECT field, field FROM tableName WHERE pk in (?, ?, ?)
     * 
     * @param clazz 数据对象
     * @param tableIndex 表下标
     * @param pks 主键
     * @return sql语句
     * @throws SQLException
     */
    public static String buildSelectByPks(EntityPK[] pks, Class<?> clazz, int tableIndex) throws SQLException {
        Field[] fields = BeansUtil.getFields(clazz);
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder whereSql = new StringBuilder();
        for (EntityPK pk : pks) {
            whereSql.append("(");
            for (int i = 0; i < pk.getPkNames().length; i++) {
                whereSql.append(pk.getPkNames()[i].getValue()).append("=")
                        .append(formatValue(pk.getPkValues()[i].getValue()));
                whereSql.append(" and ");
            }
            whereSql.delete(whereSql.length() - 5, whereSql.length());
            whereSql.append(")");
            whereSql.append(" or ");
        }
        whereSql.delete(whereSql.length() - 4, whereSql.length());

        StringBuilder SQL = new StringBuilder("SELECT ");
        for (Field field : fields) {
            SQL.append(BeansUtil.getFieldName(field)).append(",");
        }
        SQL.deleteCharAt(SQL.length() - 1);
        SQL.append(" FROM ").append(tableName);
        SQL.append(" WHERE ").append(whereSql.toString());

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 拼装sql. DELETE FROM tableName WHERE pk in (...)
     * 
     * @return DELETE语句
     * @throws SQLException
     */
    public static String buildDeleteByPks(Class<?> clazz, int tableIndex, List<EntityPK> pks) throws SQLException {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder whereSql = new StringBuilder();
        for (EntityPK pk : pks) {
            whereSql.append("(");
            for (int i = 0; i < pk.getPkNames().length; i++) {
                whereSql.append(pk.getPkNames()[i].getValue()).append("=")
                        .append(formatValue(pk.getPkValues()[i].getValue()));
                whereSql.append(" and ");
            }
            whereSql.delete(whereSql.length() - 5, whereSql.length());
            whereSql.append(")");
            whereSql.append(" or ");
        }
        whereSql.delete(whereSql.length() - 4, whereSql.length());

        StringBuilder SQL = new StringBuilder("DELETE FROM ").append(tableName);
        SQL.append(" WHERE ").append(whereSql.toString());

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 获取update PreparedStatement.
     * 
     * @param conn 数据库连接
     * @param entities 数据对象
     * @param tableIndex 分表下标
     * @return PreparedStatement
     * @throws SQLException
     */
    public static Statement getUpdate(Connection conn, List<? extends Object> entities, int tableIndex)
            throws SQLException {
        Object entity = entities.get(0);

        // 获取表名.
        String tableName = entityMetaManager.getTableName(entity, tableIndex);

        // 批量添加
        Statement st = conn.createStatement();
        Map<String, Object> entityProperty = null;
        for (Object dbEntity : entities) {
            try {
                entityProperty = BeansUtil.describe(dbEntity, false);
            } catch (Exception e) {
                throw new SQLException("解析实体对象失败", e);
            }
            // 拼装主键条件
            EntityPK entityPk = entityMetaManager.getEntityPK(dbEntity);
            StringBuilder pkWhereSql = new StringBuilder();
            for (int i = 0; i < entityPk.getPkNames().length; i++) {
                pkWhereSql.append(entityPk.getPkNames()[i].getValue());
                pkWhereSql.append("=");
                pkWhereSql.append(formatValue(entityPk.getPkValues()[i].getValue()));
                pkWhereSql.append(" and ");
            }
            pkWhereSql.delete(pkWhereSql.length() - 5, pkWhereSql.length());

            // 生成update语句.
            Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();
            StringBuilder SQL = new StringBuilder("UPDATE " + tableName + " SET ");
            Object value = null;
            for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
                value = propertyEntry.getValue();
                SQL.append(propertyEntry.getKey()).append("=");
                SQL.append(formatValue(value));
                SQL.append(",");
            }
            SQL.deleteCharAt(SQL.length() - 1);
            SQL.append(" WHERE ").append(pkWhereSql.toString());

            st.addBatch(SQL.toString());

            debugSQL(SQL.toString());
        }

        return st;
    }

    /**
     * 根据指定对象创建一个SQL语句.
     * 
     * @param conn 数据库连接引用
     * @param entity 数据对象
     * @param tableIndex 分表下标
     * @return SQL语句
     * @throws SQLException 操作失败
     */
    public static String getInsert(Object entity, int tableIndex) throws SQLException {
        // 获取表名.
        String tableName = entityMetaManager.getTableName(entity, tableIndex);

        // 批量添加
        Map<String, Object> entityProperty = null;
        try {
            // 获取需要被插入数据库的字段.
            entityProperty = BeansUtil.describe(entity, false);
        } catch (Exception e) {
            throw new SQLException("解析实体对象失败", e);
        }

        // 生成insert语句.
        Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();

        StringBuilder SQL = new StringBuilder("INSERT INTO " + tableName + "(");
        StringBuilder var = new StringBuilder();
        Object value = null;
        for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
            value = propertyEntry.getValue();
            SQL.append(propertyEntry.getKey()).append(",");
            var.append(formatValue(value));
            var.append(",");
        }
        SQL.deleteCharAt(SQL.length() - 1);
        SQL.append(") VALUES (");
        SQL.append(var.deleteCharAt(var.length() - 1).toString());
        SQL.append(")");

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 格式化数据库值. 过滤特殊字符
     */
    public static Object formatValue(Object value) {
        Object format = null;

        if (value instanceof String) {
            String content = (String) value;
            format = "'" + content.replaceAll("'", "''") + "'";
        } else if (value instanceof Character) {
            if (((int) (Character) value) == 39) {
                format = "'\\" + (Character) value + "'";
            } else {
                format = "'" + (Character) value + "'";
            }
        } else if (value instanceof Date) {
            format = "'" + sdf.format((Date) value) + "'";
        } else {
            format = value;
        }

        return format;
    }

    /**
     * 打印SQL日志.
     */
    public static void debugSQL(String sql) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(sql);
        }
    }

}
