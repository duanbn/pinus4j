package com.pinus.datalayer;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.pinus.api.SQL;
import com.pinus.api.query.IQuery;
import com.pinus.constant.Const;
import com.pinus.util.ReflectUtil;
import com.pinus.util.StringUtils;

/**
 * SQL工具类.
 * 
 * @author duanbn
 */
public class SQLBuilder {

	public static final Logger LOG = Logger.getLogger(SQLBuilder.class);

	/**
	 * select count语句缓存.
	 */
	private static final Map<String, String> _selectCountCache = new ConcurrentHashMap<String, String>();

	/**
	 * 拼装sql. SELECT pkName FROM tableName {IQuery.getSql()}
	 * 
	 * @return sql语句.
	 */
	public static String buildSelectPkByQuery(Class<?> clazz, int tableIndex, IQuery query) {
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);
		StringBuilder SQL = new StringBuilder("SELECT " + pkName + " FROM ");
		SQL.append(tableName);
		String whereSql = query.getWhereSql();
		if (StringUtils.isNotBlank(whereSql))
			SQL.append(query.getWhereSql());

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	/**
	 * 拼装sql. SELECT * FROM tableName {IQuery.getSql()}
	 * 
	 * @return sql语句.
	 */
	public static String buildSelectByQuery(Class<?> clazz, int tableIndex, IQuery query) {
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		StringBuilder SQL = new StringBuilder("SELECT * FROM ");
		SQL.append(tableName);
		String whereSql = query.getWhereSql();
		if (StringUtils.isNotBlank(whereSql))
			SQL.append(query.getWhereSql());

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	public static String buildSelectCountByQuery(Class<?> clazz, int tableIndex, IQuery query) {
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);
		StringBuilder SQL = new StringBuilder("SELECT count(" + pkName + ") FROM ");
		SQL.append(tableName);
		String whereSql = query.getWhereSql();
		if (StringUtils.isNotBlank(whereSql))
			SQL.append(query.getWhereSql());

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	public static PreparedStatement buildSelectCountGlobalBySql(Connection conn, SQL<?> sql) throws SQLException {
		// 检查sql语句中是否包含count关键字
		if (!sql.getSql().contains("count")) {
			throw new SQLException("语法错误:" + sql.getSql());
		}

		debugSQL(sql.getSql());

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
	 * 拼装sql. 根据SQL对象生成查询语句，此查询语句只能是SELECT COUNT语句.
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            查询对象
	 * @param tableIndex
	 *            分表下标
	 * 
	 * @return PreparedStatement
	 * 
	 * @throws SQLException
	 *             创建失败
	 */
	public static PreparedStatement buildSelectCountBySql(Connection conn, SQL<?> sql, int tableIndex)
			throws SQLException {
		// 检查sql语句中是否包含count关键字
		if (!sql.getSql().contains("count")) {
			throw new SQLException("语法错误:" + sql.getSql());
		}

		String s = addTableIndex(sql.getSql(), tableIndex);
		debugSQL(s);

		PreparedStatement ps = conn.prepareStatement(s);
		Object[] params = sql.getParams();
		if (params != null) {
			for (int i = 1; i <= params.length; i++) {
				ps.setObject(i, params[i - 1]);
			}
		}
		return ps;
	}

	/**
	 * 拼装sql. 只查询主键的sql.
	 * 
	 * @return SELECT pkName from tableName LIMIT start, limit
	 */
	public static String buildSelectPkWithLimit(Class<?> clazz, int tableIndex, int start, int limit) {
		String pkName = ReflectUtil.getPkName(clazz);
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);

		StringBuilder SQL = new StringBuilder("SELECT " + pkName + " FROM ");
		SQL.append(tableName).append(" LIMIT ").append(start);
		SQL.append(",").append(limit);

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	/**
	 * 拼装sql. SELECT * FROM tableName LIMIT start,limit
	 * 
	 * @return sql语句.
	 */
	public static String buildSelectWithLimit(Class<?> clazz, int tableIndex, int start, int limit) {
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);

		StringBuilder SQL = new StringBuilder("SELECT * FROM ");
		SQL.append(tableName).append(" LIMIT ").append(start);
		SQL.append(",").append(limit);

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	public static PreparedStatement buildSelectPkBySqlGlobal(Connection conn, SQL<?> sql) throws SQLException {
		String s = sql.getSql();
		String pkName = ReflectUtil.getPkName(sql.getClazz());

		s = "select " + pkName + " " + s.substring(s.indexOf("from"));

		debugSQL(s);

		PreparedStatement ps = conn.prepareStatement(s);
		Object[] params = sql.getParams();
		if (params != null) {
			for (int i = 1; i <= params.length; i++) {
				ps.setObject(i, params[i - 1]);
			}
		}
		return ps;
	}

	/**
	 * 拼装sql.
	 * 
	 * @return SELECT pkName FROM tableName WHERE {sql}.where
	 * @throws SQLException
	 */
	public static PreparedStatement buildSelectPkBySql(Connection conn, SQL<?> sql, int tableIndex) throws SQLException {
		String s = addTableIndex(sql.getSql(), tableIndex);
		String pkName = ReflectUtil.getPkName(sql.getClazz());

		s = "select " + pkName + " " + s.substring(s.indexOf("from"));

		debugSQL(s);

		PreparedStatement ps = conn.prepareStatement(s);
		Object[] params = sql.getParams();
		if (params != null) {
			for (int i = 1; i <= params.length; i++) {
				ps.setObject(i, params[i - 1]);
			}
		}
		return ps;
	}

	public static PreparedStatement buildSelectBySqlGlobal(Connection conn, SQL<?> sql) throws SQLException {
		debugSQL(sql.getSql());

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
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            查询对象
	 * @param tableIndex
	 *            分表下标
	 * 
	 * @return PreparedStatement
	 * @throws SQLException
	 */
	public static PreparedStatement buildSelectBySql(Connection conn, SQL<?> sql, int tableIndex) throws SQLException {
		String s = addTableIndex(sql.getSql(), tableIndex);

		debugSQL(s);

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
		String tableName = ReflectUtil.getTableName(clazz, -1);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder SQL = new StringBuilder("SELECT count(" + pkName + ") ").append("FROM ");
		SQL.append(tableName);
		debugSQL(SQL.toString());

		return SQL.toString();
	}

	/**
	 * 拼装sql. SELECT count(*) FROM tableName
	 * 
	 * @param clazz
	 *            数据对象class
	 * @param tableIndex
	 *            分表下标
	 * 
	 * @return SELECT count(*) FROM tableName
	 */
	public static String buildSelectCountSql(Class<?> clazz, int tableIndex) {
		String sql = _selectCountCache.get(clazz.getName() + tableIndex);
		if (sql != null) {
			debugSQL(sql);
			return sql;
		}

		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder SQL = new StringBuilder("SELECT count(" + pkName + ") ").append("FROM ");
		SQL.append(tableName);
		debugSQL(SQL.toString());

		_selectCountCache.put(clazz.getName() + tableIndex, SQL.toString());

		return SQL.toString();
	}

	/**
	 * 给定数据库查询结果集创建数据对性.
	 * 
	 * @param clazz
	 *            数据对象class
	 * @param rs
	 *            数据库查询结果集
	 * 
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
					try {
						f = clazz.getDeclaredField(fieldName);
						value = _getRsValue(rs, f, i);
						ReflectUtil.setProperty(one, fieldName, value);
					} catch (NoSuchFieldException e) {
					}
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
	 * @param clazz
	 *            数据对象
	 * @param rs
	 *            结果集
	 * @return {pkValue, Object}
	 * @throws SQLException
	 */
	public static <T> Map<Number, T> buildResultObjectAsMap(Class<T> clazz, ResultSet rs) throws SQLException {
		Map<Number, T> map = new HashMap<Number, T>();

		ResultSetMetaData rsmd = rs.getMetaData();
		T one = null;
		while (rs.next()) {
			try {
				one = (T) clazz.newInstance();
				String fieldName = null;
				String pkName = ReflectUtil.getPkName(clazz);
				Field f = null;
				Object value = null;
				Number pkValue = null;

				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					fieldName = rsmd.getColumnName(i);
					f = clazz.getDeclaredField(fieldName);
					pkValue = (Number) rs.getObject(pkName);
					value = _getRsValue(rs, f, i);
					ReflectUtil.setProperty(one, fieldName, value);
				}

				map.put(pkValue, one);
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}

		return map;
	}

	private static Object _getRsValue(ResultSet rs, Field f, int i) throws SQLException {
		Object value = rs.getObject(i);
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
		}

		return value;
	}

	/**
	 * 拼装select sql. SELECT field, field FROM tableName WHERE pk in (?, ?, ?)
	 * 
	 * @param clazz
	 *            数据对象
	 * @param tableIndex
	 *            表下标
	 * @param pks
	 *            主键
	 * 
	 * @return sql语句
	 */
	public static String buildSelectByPks(Class<?> clazz, int tableIndex, Number[] pks) {
		Field[] fields = ReflectUtil.getFields(clazz);
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder SQL = new StringBuilder("SELECT ");
		for (Field field : fields) {
			SQL.append(field.getName()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(" FROM ").append(tableName);
		SQL.append(" WHERE ").append(pkName).append(" in (");
		for (Number pk : pks) {
			SQL.append(pk).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(")");

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	/**
	 * 拼装select sql. SELECT field... FROM tableName WHERE pk = ?
	 * 
	 * @param pk
	 *            主键
	 * @param clazz
	 *            数据对象class
	 * @param tableIndex
	 *            表下标
	 * 
	 * @return sql语句
	 */
	public static String buildSelectByPk(Number pk, Class<?> clazz, int tableIndex) throws SQLException {
		Field[] fields = ReflectUtil.getFields(clazz);
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder SQL = new StringBuilder("SELECT ");
		for (Field field : fields) {
			SQL.append(field.getName()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(" FROM ").append(tableName);
		SQL.append(" WHERE ").append(pkName).append("=").append(pk.longValue());

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	/**
	 * 拼装sql. DELETE FROM tableName WHERE pk in (...)
	 * 
	 * @return DELETE语句
	 */
	public static String buildDeleteByPks(Class<?> clazz, int tableIndex, Number... pks) {
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder SQL = new StringBuilder("DELETE FROM ").append(tableName);
		SQL.append(" WHERE ").append(pkName).append(" IN (");
		for (Number pk : pks) {
			if (pk.longValue() > 0)
				SQL.append(pk.longValue()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(")");

		debugSQL(SQL.toString());

		return SQL.toString();
	}

	/**
	 * 构建update sql.
	 * 
	 * @param entityProperty
	 *            数据对象反射
	 * @param pkName
	 *            主键字段名
	 * @param tableName
	 *            数据表名
	 * 
	 * @return update sql语句
	 */
	private static String _buildUpdateSql(Map<String, Object> entityProperty, String pkName, String tableName) {
		// 生成update语句.
		Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();
		StringBuilder SQL = new StringBuilder("UPDATE " + tableName + " SET ");
		for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
			SQL.append(propertyEntry.getKey()).append("=?").append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(" WHERE ").append(pkName).append("=?");

		return SQL.toString();
	}

	/**
	 * 获取update PreparedStatement.
	 * 
	 * @param conn
	 *            数据库连接
	 * @param entities
	 *            数据对象
	 * @param tableIndex
	 *            分表下标
	 * 
	 * @return PreparedStatement
	 * @throws SQLException
	 */
	public static PreparedStatement getUpdate(Connection conn, List<? extends Object> entities, int tableIndex)
			throws SQLException {
		Object entity = entities.get(0);
		Class<?> entityClass = entity.getClass();
		Map<String, Object> entityProperty;
		try {
			entityProperty = ReflectUtil.describeWithoutUpdateTime(entity, true);
		} catch (Exception e) {
			throw new SQLException("解析实体对象失败", e);
		}

		// 获取表名.
		String tableName = ReflectUtil.getTableName(entity, tableIndex);
		// 获取主键字段名
		String pkName = ReflectUtil.getPkName(entityClass);
		// 上层调用已经判断了entities正确性
		entityProperty.remove(pkName);
		String sql = _buildUpdateSql(entityProperty, pkName, tableName);

		// 批量添加
		PreparedStatement ps = conn.prepareStatement(sql);
		Set<Map.Entry<String, Object>> propertyEntrySet = null;
		for (Object dbEntity : entities) {
			try {
				entityProperty = ReflectUtil.describeWithoutUpdateTime(dbEntity, true);
			} catch (Exception e) {
				throw new SQLException("解析实体对象失败", e);
			}
			// 将主键放在最后一个参数
			Object pkValue = entityProperty.get(pkName);
			entityProperty.remove(pkName);
			propertyEntrySet = entityProperty.entrySet();

			int i = 1;
			Object param = null;
			for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
				param = propertyEntry.getValue();
				if (param instanceof Character) {
					ps.setString(i, String.valueOf(param));
				} else {
					ps.setObject(i, param);
				}
				i++;
			}
			ps.setObject(i, pkValue);
			ps.addBatch();

			debugUpdate(entityProperty, tableName, pkName, pkValue);
		}
		return ps;
	}

	/**
	 * 拼装insert sql.
	 * 
	 * @param entityProperty
	 *            数据对象属性.
	 * @param tableName
	 *            表名
	 * 
	 * @return INSERT INTO tableName(field...) VALUES(?, ?, ?, ?)
	 */
	private static String _buildInsertSql(Map<String, Object> entityProperty, String tableName) {
		// 生成insert语句.
		Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();

		StringBuilder SQL = new StringBuilder("INSERT INTO " + tableName + "(");
		StringBuilder var = new StringBuilder();
		for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
			SQL.append(propertyEntry.getKey()).append(",");
			var.append("?,");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(") VALUES (");
		SQL.append(var.deleteCharAt(var.length() - 1).toString());
		SQL.append(")");

		return SQL.toString();
	}

	/**
	 * 根据指定对象创建一个SQL语句.
	 * 
	 * @param conn
	 *            数据库连接引用
	 * @param entities
	 *            数据对象
	 * @param tableIndex
	 *            分表下标
	 * 
	 * @return SQL语句
	 * 
	 * @throws 操作失败
	 */
	public static PreparedStatement getInsert(Connection conn, List<? extends Object> entities, int tableIndex)
			throws SQLException {
		Object entity = entities.get(0);
		Map<String, Object> entityProperty;
		try {
			entityProperty = ReflectUtil.describe(entity);
		} catch (Exception e) {
			throw new SQLException("解析实体对象失败", e);
		}

		// 获取表名.
		String tableName = ReflectUtil.getTableName(entity, tableIndex);
		// 上层调用已经判断了entities正确性
		String sql = _buildInsertSql(entityProperty, tableName);

		// 批量添加
		PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		Set<Map.Entry<String, Object>> propertyEntrySet = null;
		for (Object dbEntity : entities) {
			try {
				entityProperty = ReflectUtil.describe(dbEntity);
			} catch (Exception e) {
				throw new SQLException("解析实体对象失败", e);
			}
			propertyEntrySet = entityProperty.entrySet();

			int i = 1;
			Object param = null;
			for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
				param = propertyEntry.getValue();
				if (param instanceof Character) {
					ps.setString(i, String.valueOf(param));
				} else {
					ps.setObject(i, param);
				}
				i++;
			}
			ps.addBatch();

			debugInsert(entityProperty, tableName);
		}
		return ps;
	}

	/**
	 * 关闭数据相关资源.
	 * 
	 * @param conn
	 *            数据库连接
	 */
	public static void close(Connection conn) {
		close(conn, null);
	}

	/**
	 * 关闭数据库相关资源.
	 * 
	 * @param conn
	 *            数据库连接
	 * @param ps
	 *            PreparedStatement对象
	 */
	public static void close(Connection conn, PreparedStatement ps) {
		close(conn, ps, null);
	}

	/**
	 * 关闭数据库相关资源.
	 * 
	 * @param conn
	 *            数据库连接
	 * @param ps
	 *            PreparedStatement对象
	 * @param rs
	 *            查询结果集
	 */
	public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			LOG.error(e);
		}
	}

	/**
	 * 解析sql的表名并增加分表下标.
	 * 
	 * @param sql
	 * @param tableIndex
	 * @return 带分表下标的表名
	 */
	private static String addTableIndex(String sql, int tableIndex) {
		// 解析sql并给表名加上分表下标
		int beginIndex = sql.indexOf("from") + 5;
		String afterFrom = sql.substring(beginIndex);
		String tableName = null;
		if (afterFrom.contains(" ")) {
			tableName = afterFrom.substring(0, afterFrom.indexOf(" "));
		} else {
			tableName = afterFrom;
		}
		return sql.replaceAll(tableName, tableName + tableIndex);
	}

	/**
	 * 打印SQL日志.
	 */
	public static void debugSQL(String sql) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(sql);
		}
	}

	/**
	 * 打印Insert日志
	 * 
	 * @param propertyEntry
	 * @param tableName
	 */
	public static void debugInsert(Map<String, Object> propertyEntry, String tableName) {
		StringBuilder SQL = new StringBuilder("INSERT INTO " + tableName + "(");
		for (Map.Entry<String, Object> property : propertyEntry.entrySet()) {
			SQL.append(property.getKey()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(") VALUES (");
		for (Map.Entry<String, Object> property : propertyEntry.entrySet()) {
			SQL.append(property.getValue()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(")");

		debugSQL(SQL.toString());
	}

	/**
	 * 打印Update日志
	 * 
	 * @param entityProperty
	 * @param tableName
	 * @param pkName
	 * @param pkValue
	 */
	public static void debugUpdate(Map<String, Object> entityProperty, String tableName, String pkName, Object pkValue) {
		Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();
		StringBuilder SQL = new StringBuilder("UPDATE " + tableName + " SET ");
		for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
			SQL.append(propertyEntry.getKey()).append("=").append(propertyEntry.getValue()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(" WHERE ").append(pkName).append("=").append(pkValue);

		debugSQL(SQL.toString());
	}

}
