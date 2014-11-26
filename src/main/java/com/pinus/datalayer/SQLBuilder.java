package com.pinus.datalayer;

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

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
	 * 拼装sql. SELECT {fields} FROM tableName {IQuery.getSql()}
	 * 
	 * @return sql语句.
	 */
	public static String buildSelectByQuery(Class<?> clazz, int tableIndex, IQuery query) {
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);

		StringBuilder fields = new StringBuilder();
		if (query.hasQueryFields()) {
			for (String field : query.getFields()) {
				fields.append(field).append(",");
			}
			fields.deleteCharAt(fields.length() - 1);
		} else {
			fields.append("*");
		}

		StringBuilder SQL = new StringBuilder("SELECT ");
		SQL.append(fields.toString()).append(" FROM ");
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
		String tableName = ReflectUtil.getTableName(clazz, -1);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder SQL = new StringBuilder("SELECT count(" + pkName + ") ").append("FROM ");
		SQL.append(tableName);
		if (query != null) {
			SQL.append(query.getWhereSql());
		}
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
	 * @param rs
	 *            数据库查询结果集
	 * 
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
	 * @param query
	 *            保证in顺序
	 * 
	 * @return sql语句
	 */
	public static String buildSelectByPks(Class<?> clazz, int tableIndex, Number[] pks) {
		Field[] fields = ReflectUtil.getFields(clazz);
		String tableName = ReflectUtil.getTableName(clazz, tableIndex);
		String pkName = ReflectUtil.getPkName(clazz);

		StringBuilder sqlInValue = new StringBuilder();
		for (Number pk : pks) {
			sqlInValue.append(pk).append(",");
		}
		sqlInValue.deleteCharAt(sqlInValue.length() - 1);

		StringBuilder SQL = new StringBuilder("SELECT ");
		for (Field field : fields) {
			SQL.append(field.getName()).append(",");
		}
		SQL.deleteCharAt(SQL.length() - 1);
		SQL.append(" FROM ").append(tableName);
		SQL.append(" WHERE ").append(pkName).append(" in (");
		SQL.append(sqlInValue.toString());
		SQL.append(") order by field(");
		SQL.append(pkName).append(",").append(sqlInValue.toString());
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
	public static String buildDeleteByPks(Class<?> clazz, int tableIndex, List<? extends Number> pks) {
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
	public static Statement getUpdate(Connection conn, List<? extends Object> entities, int tableIndex)
			throws SQLException {
		Object entity = entities.get(0);
		Class<?> entityClass = entity.getClass();

		// 获取表名.
		String tableName = ReflectUtil.getTableName(entity, tableIndex);
		// 获取主键名.
		String pkName = ReflectUtil.getPkName(entityClass);

		// 批量添加
		Statement st = conn.createStatement();
		Map<String, Object> entityProperty = null;
		for (Object dbEntity : entities) {
			try {
				entityProperty = ReflectUtil.describeWithoutUpdateTime(dbEntity, true);
			} catch (Exception e) {
				throw new SQLException("解析实体对象失败", e);
			}
			// 将主键放在最后一个参数
			Object pkValue = entityProperty.get(pkName);
			entityProperty.remove(pkName);

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
			SQL.append(" WHERE ").append(pkName).append("=").append(pkValue);

			st.addBatch(SQL.toString());

			debugSQL(SQL.toString());
		}

		return st;
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
	public static Statement getInsert(Connection conn, List<? extends Object> entities, int tableIndex)
			throws SQLException {
		Object entity = entities.get(0);
		// 获取表名.
		String tableName = ReflectUtil.getTableName(entity, tableIndex);

		// 批量添加
		Statement st = conn.createStatement();
		Map<String, Object> entityProperty = null;
		for (Object dbEntity : entities) {
			try {
				// 获取需要被插入数据库的字段.
				entityProperty = ReflectUtil.describe(dbEntity, true);
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

			st.addBatch(SQL.toString());

			debugSQL(SQL.toString());
		}
		return st;
	}

	/**
	 * 格式化数据库值.
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
	 * 关闭数据库Statement.
	 */
	public static void close(Statement st) {
		try {
			if (st != null) {
				st.close();
			}
		} catch (SQLException e) {
			LOG.error(e);
		}
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
	 * 打印SQL日志.
	 */
	public static void debugSQL(String sql) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(sql);
		}
	}

}
