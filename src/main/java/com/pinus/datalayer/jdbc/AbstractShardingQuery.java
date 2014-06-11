package com.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pinus.api.SQL;
import com.pinus.api.query.IQuery;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.DB;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.constant.Const;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.datalayer.SlowQueryLogger;
import com.pinus.exception.DBOperationException;
import com.pinus.util.ReflectUtil;

/**
 * 分库分表查询抽象类. 此类封装了分库分表查询的公共操作. 子类可以针对主库、从库实现相关的查询.
 * 
 * @author duanbn
 */
public abstract class AbstractShardingQuery {

	/**
	 * 主缓存.
	 */
	protected IPrimaryCache primaryCache;

	/**
	 * 判断缓存是否可用
	 * 
	 * @return true:启用cache, false:不启用
	 */
	protected boolean isCacheAvailable(Class<?> clazz) {
		return primaryCache != null && ReflectUtil.isCache(clazz);
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
	private Number _selectCountGlobal(Connection conn, Class<?> clazz) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectCountGlobalSql(clazz);
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
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}
	}

	/**
	 * 带缓存的获取全局表count
	 * 
	 * @param conn
	 * @param clusterName
	 * @param clazz
	 * @return count数
	 */
	protected Number selectCountGlobalWithCache(DBConnectionInfo dbConnInfo, String clusterName, Class<?> clazz) {
		String tableName = ReflectUtil.getTableName(clazz);

		// 操作缓存
		if (isCacheAvailable(clazz)) {
			long count = primaryCache.getCountGlobal(clusterName, tableName);
			if (count > 0) {
				return count;
			}
		}

		long count = 0;
		Connection conn = null;
		try {
			conn = dbConnInfo.getDatasource().getConnection();
			count = _selectCountGlobal(conn, clazz).longValue();
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		// 操作缓存
		if (isCacheAvailable(clazz) && count > 0)
			primaryCache.setCountGlobal(clusterName, tableName, count);

		return count;
	}

	/**
	 * 获取分库分表记录总数.
	 * 
	 * @param db
	 *            分库分表
	 * @param clazz
	 *            数据对象
	 * 
	 * @return 表记录总数
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	private Number _selectCount(DB db, Class<?> clazz) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectCountSql(clazz, db.getTableIndex());
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_COUNT) {
				SlowQueryLogger.write(db, sql, constTime);
			}

			long count = -1;
			if (rs.next()) {
				count = rs.getLong(1);
			}
			return count;
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}
	}

	/**
	 * getCount加入缓存
	 * 
	 * @param db
	 * @param clazz
	 * @return count数
	 */
	protected Number selectCountWithCache(DB db, Class<?> clazz) {
		// 操作缓存
		if (isCacheAvailable(clazz)) {
			long count = primaryCache.getCount(db);
			if (count > 0) {
				return count;
			}
		}

		long count = _selectCount(db, clazz).longValue();

		// 操作缓存
		if (isCacheAvailable(clazz) && count > 0)
			primaryCache.setCount(db, count);

		return count;
	}

	/**
	 * 根据sql获取count值，没有缓存
	 * 
	 * @param conn
	 * @param sql
	 * @return
	 */
	protected Number selectCountGlobal(Connection conn, SQL<?> sql) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = SQLBuilder.buildSelectCountGlobalBySql(conn, sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_COUNT) {
				SlowQueryLogger.write(conn, sql, constTime);
			}

			if (rs.next()) {
				return rs.getLong(1);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return -1;
	}

	/**
	 * 根据条件获取分库分表记录数. 没有缓存
	 * 
	 * @param db
	 *            分库分表
	 * @param sql
	 *            sql语句
	 * 
	 * @return 记录数
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	protected Number selectCount(DB db, SQL<?> sql) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			ps = SQLBuilder.buildSelectCountBySql(conn, sql, db.getTableIndex());
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_COUNT) {
				SlowQueryLogger.write(db, sql, constTime);
			}

			if (rs.next()) {
				return rs.getLong(1);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return -1;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findByPk相关
	// //////////////////////////////////////////////////////////////////////////////////////
	private <T> T _selectByPkGlobal(Connection conn, Number pk, Class<T> clazz) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectByPk(pk, clazz, -1);
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
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return null;
	}

	protected <T> T selectByPkWithCache(Connection conn, String clusterName, Number pk, Class<T> clazz) {
		String tableName = ReflectUtil.getTableName(clazz);

		T data = null;
		if (isCacheAvailable(clazz)) {
			data = primaryCache.getGlobal(clusterName, tableName, pk);
			if (data == null) {
				data = _selectByPkGlobal(conn, pk, clazz);
				if (data != null) {
					primaryCache.putGlobal(clusterName, tableName, pk, data);
				}
			}
		} else {
			data = _selectByPkGlobal(conn, pk, clazz);
		}

		return data;
	}

	/**
	 * 一个主分库分表, 根据主键查询.
	 * 
	 * @param pk
	 *            主键
	 * @param clazz
	 *            数据对象类型
	 * 
	 * @return 查询结果，找不到返回null
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	private <T> T _selectByPk(DB db, Number pk, Class<T> clazz) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectByPk(pk, clazz, db.getTableIndex());
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PK) {
				SlowQueryLogger.write(db, sql, constTime);
			}

			List<T> result = SQLBuilder.buildResultObject(clazz, rs);
			if (!result.isEmpty()) {
				return result.get(0);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return null;
	}

	/**
	 * findByPk加入缓存.
	 * 
	 * @param db
	 * @param pk
	 * @param clazz
	 * @return 查询结果
	 */
	protected <T> T selectByPkWithCache(DB db, Number pk, Class<T> clazz) {
		T data = null;
		if (isCacheAvailable(clazz)) {
			data = primaryCache.get(db, pk);
			if (data == null) {
				data = _selectByPk(db, pk, clazz);
			}
			if (data != null) {
				primaryCache.put(db, pk, data);
				return data;
			}
		} else {
			data = _selectByPk(db, pk, clazz);
		}

		return data;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findByPks相关
	// //////////////////////////////////////////////////////////////////////////////////////
	private <T> List<T> _selectByPksGlobal(Connection conn, Class<T> clazz, Number[] pks) {
		List<T> result = new ArrayList<T>(1);

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectByPks(clazz, -1, pks);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			result = SQLBuilder.buildResultObject(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	private <T> Map<Number, T> _selectByPksGlobalWithMap(Connection conn, Class<T> clazz, Number[] pks) {
		Map<Number, T> result = new HashMap<Number, T>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectByPks(clazz, -1, pks);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			result = SQLBuilder.buildResultObjectAsMap(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	protected <T> List<T> selectByPksGlobalWithCache(Connection conn, String clusterName, Class<T> clazz, Number[] pks) {
		List<T> result = new ArrayList<T>();

		if (pks == null || pks.length == 0) {
			return result;
		}

		if (isCacheAvailable(clazz)) { // 缓存可用
			String tableName = ReflectUtil.getTableName(clazz);
			List<T> hitResult = primaryCache.getGlobal(clusterName, tableName, pks);
			if (hitResult != null && !hitResult.isEmpty()) {
				if (hitResult.size() == pks.length) {
					result = hitResult;
				} else {
					try {
						// 计算没有命中缓存的主键
						Map<Number, T> hitMap = _getPkValues(hitResult);
						List<Number> noHitPkList = new ArrayList<Number>();
						for (Number pk : pks) {
							if (hitMap.get(pk) == null) {
								noHitPkList.add(pk);
							}
						}
						Number[] noHitPks = noHitPkList.toArray(new Number[noHitPkList.size()]);

						// 从数据库中查询没有命中缓存的数据
						Map<Number, T> noHitMap = _selectByPksGlobalWithMap(conn, clazz, noHitPks);
						if (!noHitMap.isEmpty()) {
							primaryCache.putGlobal(clusterName, tableName, noHitMap);
						}

						// 为了保证pks的顺序
						for (Number pk : pks) {
							if (hitMap.get(pk) != null) {
								result.add(hitMap.get(pk));
							} else {
								result.add(noHitMap.get(pk));
							}
						}
					} catch (Exception e) {
						result = _selectByPksGlobal(conn, clazz, pks);
					}
				}
			} else {
				result = _selectByPksGlobal(conn, clazz, pks);
				primaryCache.putGlobal(clusterName, tableName, result);
			}
		} else {
			result = _selectByPksGlobal(conn, clazz, pks);
		}

		return result;
	}

	/**
	 * 一个主分库分表, 根据多个主键查询.
	 * 
	 * @param db
	 *            分库分表
	 * @param clazz
	 *            数据对象类型
	 * @param pks
	 *            主键
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	private <T> List<T> _selectByPks(DB db, Class<T> clazz, Number[] pks) {
		List<T> result = new ArrayList<T>(1);

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectByPks(clazz, db.getTableIndex(), pks);
			long begin = System.currentTimeMillis();
			ps = conn.prepareStatement(sql);
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			rs = ps.executeQuery();
			result = SQLBuilder.buildResultObject(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	private <T> Map<Number, T> selectByPksWithMap(DB db, Class<T> clazz, Number[] pks) {
		Map<Number, T> result = new HashMap<Number, T>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectByPks(clazz, db.getTableIndex(), pks);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			result = SQLBuilder.buildResultObjectAsMap(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	/**
	 * findByPks加入缓存
	 * 
	 * @param db
	 * @param clazz
	 * @param pks
	 * @return 查询结果
	 */
	protected <T> List<T> selectByPksWithCache(DB db, Class<T> clazz, Number[] pks) {
		List<T> result = new ArrayList<T>();
		if (pks.length == 0 || pks == null) {
			return result;
		}

		if (isCacheAvailable(clazz)) { // 缓存可用
			List<T> hitResult = primaryCache.get(db, pks);
			if (hitResult != null && !hitResult.isEmpty()) {
				if (hitResult.size() == pks.length) {
					result = hitResult;
				} else {
					try {
						// 计算没有命中缓存的主键
						Map<Number, T> hitMap = _getPkValues(hitResult);
						List<Number> noHitPkList = new ArrayList<Number>();
						for (Number pk : pks) {
							if (hitMap.get(pk) == null) {
								noHitPkList.add(pk);
							}
						}
						Number[] noHitPks = noHitPkList.toArray(new Number[noHitPkList.size()]);

						// 从数据库中查询没有命中缓存的数据
						Map<Number, T> noHitMap = selectByPksWithMap(db, clazz, noHitPks);
						if (!noHitMap.isEmpty()) {
							primaryCache.put(db, noHitMap);
						}

						// 为了保证pks的顺序
						for (Number pk : pks) {
							if (hitMap.get(pk) != null) {
								result.add(hitMap.get(pk));
							} else {
								result.add(noHitMap.get(pk));
							}
						}
					} catch (Exception e) {
						result = _selectByPks(db, clazz, pks);
					}
				}
			} else {
				result = _selectByPks(db, clazz, pks);
				primaryCache.put(db, pks, result);
			}
		} else {
			result = _selectByPks(db, clazz, pks);
		}

		return result;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findMore相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected <T> List<T> selectMoreGlobal(Connection conn, Class<T> clazz, int start, int limit) {
		List<T> result = new ArrayList<T>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectWithLimit(clazz, -1, start, limit);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_MORE) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			result = SQLBuilder.buildResultObject(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	/**
	 * 一个主分库分表, 无条件查询全部.
	 * 
	 * @param db
	 *            分库分表
	 * @param clazz
	 *            数据对象类型
	 * @param start
	 *            分页偏移量
	 * @param limit
	 *            分页大小
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	protected <T> List<T> selectMore(DB db, Class<T> clazz, int start, int limit) {
		List<T> result = new ArrayList<T>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectWithLimit(clazz, db.getTableIndex(), start, limit);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_MORE) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			result = SQLBuilder.buildResultObject(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findBySql相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected <T> List<T> selectBySqlGlobal(Connection conn, SQL<T> sql) {
		List<T> result = new ArrayList<T>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = SQLBuilder.buildSelectBySqlGlobal(conn, sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_SQL) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			result = (List<T>) SQLBuilder.buildResultObject(sql.getClazz(), rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	/**
	 * 一个主分库分表, 根据条件查询.
	 * 
	 * @param db
	 *            分库分表
	 * @param sql
	 *            查询语句
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	protected <T> List<T> selectBySql(DB db, SQL<T> sql) {
		List<T> result = new ArrayList<T>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			ps = SQLBuilder.buildSelectBySql(conn, sql, db.getTableIndex());
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_SQL) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			result = (List<T>) SQLBuilder.buildResultObject(sql.getClazz(), rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findByQuery相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected <T> List<T> selectByQueryGlobal(Connection conn, IQuery query, Class<T> clazz) {
		List<T> result = new ArrayList<T>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectByQuery(clazz, -1, query);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_QUERY) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			result = (List<T>) SQLBuilder.buildResultObject(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	/**
	 * 根据查询条件对象进行查询.
	 * 
	 * @param db
	 *            分库分表
	 * @param query
	 *            查询条件
	 * @param clazz
	 *            数据对象class
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	protected <T> List<T> selectByQuery(DB db, IQuery query, Class<T> clazz) {
		List<T> result = new ArrayList<T>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectByQuery(clazz, db.getTableIndex(), query);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_QUERY) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			result = (List<T>) SQLBuilder.buildResultObject(clazz, rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// getPk相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected Number[] selectPksMoreGlobal(Connection conn, Class<?> clazz, int start, int limit) {
		List<Number> result = new ArrayList<Number>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectPkWithLimit(clazz, -1, start, limit);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_MORE) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			while (rs.next()) {
				result.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	/**
	 * 分页获取主键
	 * 
	 * @param db
	 * @param clazz
	 * @param start
	 * @param limit
	 * @return pk值
	 */
	protected Number[] selectPksMore(DB db, Class<?> clazz, int start, int limit) {
		List<Number> result = new ArrayList<Number>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			String sql = SQLBuilder.buildSelectPkWithLimit(clazz, db.getTableIndex(), start, limit);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_MORE) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			while (rs.next()) {
				result.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	protected <T> Number[] selectPksBySqlGlobal(Connection conn, SQL<T> sql) {
		List<Number> result = new ArrayList<Number>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = SQLBuilder.buildSelectPkBySqlGlobal(conn, sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			while (rs.next()) {
				result.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	/**
	 * 根据SQL查询主键
	 * 
	 * @param db
	 * @param sql
	 * @return pk值
	 */
	protected <T> Number[] selectPksBySql(DB db, SQL<T> sql) {
		List<Number> result = new ArrayList<Number>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();
			ps = SQLBuilder.buildSelectPkBySql(conn, sql, db.getTableIndex());
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			while (rs.next()) {
				result.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	protected <T> Number[] selectPksByQueryGlobal(Connection conn, IQuery query, Class<T> clazz) {
		List<Number> result = new ArrayList<Number>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = SQLBuilder.buildSelectPkByQuery(clazz, -1, query);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(conn, sql, constTime);
			}
			while (rs.next()) {
				result.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	/**
	 * 根据查询条件查询主键.
	 * 
	 * @param db
	 * @param query
	 * @param clazz
	 * @return pk值
	 */
	protected <T> Number[] selectPksByQuery(DB db, IQuery query, Class<T> clazz) {
		List<Number> result = new ArrayList<Number>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDbConn();

			String sql = SQLBuilder.buildSelectPkByQuery(clazz, db.getTableIndex(), query);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			while (rs.next()) {
				result.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	public void setPrimaryCache(IPrimaryCache primaryCache) {
		this.primaryCache = primaryCache;
	}

	/**
	 * 获取列表的主键.
	 * 
	 * @param entities
	 * @return pk值
	 * @throws Exception
	 *             获取pk值失败
	 */
	private <T> Map<Number, T> _getPkValues(List<T> entities) {
		Map<Number, T> map = new HashMap<Number, T>();

		Number pkValue = null;
		for (T entity : entities) {
			pkValue = ReflectUtil.getPkValue(entity);
			map.put(pkValue, entity);
		}

		return map;
	}

}
