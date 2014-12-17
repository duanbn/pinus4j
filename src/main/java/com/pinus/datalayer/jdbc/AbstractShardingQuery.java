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
import com.pinus.cache.ISecondCache;
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
	 * 一级缓存.
	 */
	protected IPrimaryCache primaryCache;

	/**
	 * 二级缓存.
	 */
	protected ISecondCache secondCache;

	/**
	 * 判断一级缓存是否可用
	 * 
	 * @return true:启用cache, false:不启用
	 */
	protected boolean isCacheAvailable(Class<?> clazz, boolean useCache) {
		return primaryCache != null && ReflectUtil.isCache(clazz) && useCache;
	}

	/**
	 * 判断二级缓存是否可用
	 * 
	 * @return true:启用cache, false:不启用
	 */
	protected boolean isSecondCacheAvailable(Class<?> clazz, boolean useCache) {
		return secondCache != null && ReflectUtil.isCache(clazz) && useCache;
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
	private Number _selectGlobalCount(Connection conn, Class<?> clazz) {
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

	protected Number selectGlobalCount(IQuery query, DBConnectionInfo dbConnInfo, String clusterName, Class<?> clazz) {
		long count = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dbConnInfo.getDatasource().getConnection();
			String sql = SQLBuilder.buildSelectCountGlobalSql(clazz, query);
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
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn, ps, rs);
		}
		return count;
	}

	/**
	 * 带缓存的获取全局表count
	 * 
	 * @param conn
	 * @param clusterName
	 * @param clazz
	 * @return count数
	 */
	protected Number selectGlobalCountWithCache(DBConnectionInfo dbConnInfo, String clusterName, Class<?> clazz,
			boolean useCache) {
		String tableName = ReflectUtil.getTableName(clazz);

		// 操作缓存
		if (isCacheAvailable(clazz, useCache)) {
			long count = primaryCache.getCountGlobal(clusterName, tableName);
			if (count > 0) {
				return count;
			}
		}

		long count = 0;
		Connection conn = null;
		try {
			conn = dbConnInfo.getDatasource().getConnection();
			count = _selectGlobalCount(conn, clazz).longValue();
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		// 操作缓存
		if (isCacheAvailable(clazz, useCache) && count > 0)
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
			conn = db.getDatasource().getConnection();
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
			SQLBuilder.close(conn, ps, rs);
		}
	}

	/**
	 * getCount加入缓存
	 * 
	 * @param db
	 * @param clazz
	 * @return count数
	 */
	protected Number selectCountWithCache(DB db, Class<?> clazz, boolean useCache) {
		// 操作缓存
		if (isCacheAvailable(clazz, useCache)) {
			long count = primaryCache.getCount(db);
			if (count > 0) {
				return count;
			}
		}

		long count = _selectCount(db, clazz).longValue();

		// 操作缓存
		if (isCacheAvailable(clazz, useCache) && count > 0)
			primaryCache.setCount(db, count);

		return count;
	}

	/**
	 * 根据查询条件查询记录数.
	 * 
	 * @param db
	 *            分库分表引用
	 * @param clazz
	 *            实体对象
	 * @param query
	 *            查询条件
	 * 
	 * @return 记录数
	 */
	protected Number selectCount(DB db, Class<?> clazz, IQuery query) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDatasource().getConnection();
			String sql = SQLBuilder.buildSelectCountByQuery(clazz, db.getTableIndex(), query);
			ps = conn.prepareStatement(sql);
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
			SQLBuilder.close(conn, ps, rs);
		}

		return -1;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findByPk相关
	// //////////////////////////////////////////////////////////////////////////////////////
	private <T> T _selectGlobalByPk(Connection conn, Number pk, Class<T> clazz) {
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

	protected <T> T selectByPkWithCache(Connection conn, String clusterName, Number pk, Class<T> clazz, boolean useCache) {
		String tableName = ReflectUtil.getTableName(clazz);

		T data = null;
		if (isCacheAvailable(clazz, useCache)) {
			data = primaryCache.getGlobal(clusterName, tableName, pk);
			if (data == null) {
				data = _selectGlobalByPk(conn, pk, clazz);
				if (data != null) {
					primaryCache.putGlobal(clusterName, tableName, pk, data);
				}
			}
		} else {
			data = _selectGlobalByPk(conn, pk, clazz);
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
			conn = db.getDatasource().getConnection();
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
			SQLBuilder.close(conn, ps, rs);
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
	protected <T> T selectByPkWithCache(DB db, Number pk, Class<T> clazz, boolean useCache) {
		T data = null;
		if (isCacheAvailable(clazz, useCache)) {
			data = primaryCache.get(db, pk);
			if (data == null) {
				data = _selectByPk(db, pk, clazz);
				if (data != null) {
					primaryCache.put(db, pk, data);
				}
			}
		} else {
			data = _selectByPk(db, pk, clazz);
		}

		return data;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findByPks相关
	// //////////////////////////////////////////////////////////////////////////////////////
	private <T> List<T> _selectGlobalByPks(Connection conn, Class<T> clazz, Number[] pks) {
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

	private <T> Map<Number, T> _selectGlobalByPksWithMap(Connection conn, Class<T> clazz, Number[] pks) {
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

	protected <T> List<T> selectGlobalByPksWithCache(Connection conn, String clusterName, Class<T> clazz, Number[] pks,
			boolean useCache) {
		List<T> result = new ArrayList<T>();

		if (pks == null || pks.length == 0) {
			return result;
		}

		if (isCacheAvailable(clazz, useCache)) { // 缓存可用
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
						Map<Number, T> noHitMap = _selectGlobalByPksWithMap(conn, clazz, noHitPks);
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
						result = _selectGlobalByPks(conn, clazz, pks);
					}
				}
			} else {
				result = _selectGlobalByPks(conn, clazz, pks);
				primaryCache.putGlobal(clusterName, tableName, result);
			}
		} else {
			result = _selectGlobalByPks(conn, clazz, pks);
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
			conn = db.getDatasource().getConnection();
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
			SQLBuilder.close(conn, ps, rs);
		}

		return result;
	}

	private <T> Map<Number, T> selectByPksWithMap(DB db, Class<T> clazz, Number[] pks) {
		Map<Number, T> result = new HashMap<Number, T>();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDatasource().getConnection();
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
			SQLBuilder.close(conn, ps, rs);
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
	protected <T> List<T> selectByPksWithCache(DB db, Class<T> clazz, Number[] pks, boolean useCache) {
		List<T> result = new ArrayList<T>();
		if (pks.length == 0 || pks == null) {
			return result;
		}

		if (isCacheAvailable(clazz, useCache)) { // 缓存可用
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
	// findBySql相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected List<Map<String, Object>> selectGlobalBySql(Connection conn, SQL sql) {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
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
			result = (List<Map<String, Object>>) SQLBuilder.buildResultObject(rs);
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
	protected List<Map<String, Object>> selectBySql(DB db, SQL sql) {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = db.getDatasource().getConnection();
			ps = SQLBuilder.buildSelectBySql(conn, sql, db.getTableIndex());

			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_SQL) {
				SlowQueryLogger.write(db, sql, constTime);
			}

			result = (List<Map<String, Object>>) SQLBuilder.buildResultObject(rs);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn, ps, rs);
		}

		return result;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// findByQuery相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected <T> List<T> selectGlobalByQuery(Connection conn, IQuery query, Class<T> clazz) {
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
			conn = db.getDatasource().getConnection();
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
			SQLBuilder.close(conn, ps, rs);
		}

		return result;
	}

	// //////////////////////////////////////////////////////////////////////////////////////
	// getPk相关
	// //////////////////////////////////////////////////////////////////////////////////////
	protected <T> Number[] selectGlobalPksByQuery(Connection conn, IQuery query, Class<T> clazz) {
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
				result.add((Number) rs.getObject(1));
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
			conn = db.getDatasource().getConnection();

			String sql = SQLBuilder.buildSelectPkByQuery(clazz, db.getTableIndex(), query);
			ps = conn.prepareStatement(sql);
			long begin = System.currentTimeMillis();
			rs = ps.executeQuery();
			long constTime = System.currentTimeMillis() - begin;
			if (constTime > Const.SLOWQUERY_PKS) {
				SlowQueryLogger.write(db, sql, constTime);
			}
			while (rs.next()) {
				result.add((Number) rs.getObject(1));
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn, ps, rs);
		}

		return result.toArray(new Number[result.size()]);
	}

	public void setPrimaryCache(IPrimaryCache primaryCache) {
		this.primaryCache = primaryCache;
	}

	public void setSecondCache(ISecondCache secondCache) {
		this.secondCache = secondCache;
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
