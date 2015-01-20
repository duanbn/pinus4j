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

package org.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinus.api.SQL;
import org.pinus.api.query.IQuery;
import org.pinus.cluster.beans.DBConnectionInfo;
import org.pinus.datalayer.IGlobalMasterQuery;
import org.pinus.datalayer.SQLBuilder;
import org.pinus.exception.DBClusterException;
import org.pinus.exception.DBOperationException;
import org.pinus.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * global master database query implements.
 *
 * @author duanbn
 * @since 0.7.1
 */
public class GlobalJdbcMasterQueryImpl extends AbstractJdbcQuery implements IGlobalMasterQuery {

    public static final Logger LOG = LoggerFactory.getLogger(GlobalJdbcMasterQueryImpl.class);

    @Override
	public <T> T findGlobalOneByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz, boolean useCache) {
		List<T> entities = findGlobalByQueryFromMaster(query, clusterName, clazz, useCache);

		if (entities.isEmpty()) {
			return null;
		}

		try {
			if (query.hasQueryFields()) {
				T obj = (T) ReflectUtil.cloneWithGivenField(entities.get(0), query.getFields());
				return obj;
			} else {
				T obj = entities.get(0);
				return obj;
			}
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public Number getGlobalCountFromMaster(String clusterName, Class<?> clazz, boolean useCache) {
		DBConnectionInfo globalConnection;
		try {
			globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectGlobalCountWithCache(globalConnection, clusterName, clazz, useCache).longValue();

		return count;
	}

	@Override
	public Number getGlobalCountFromMaster(IQuery query, String clusterName, Class<?> clazz) {
		DBConnectionInfo globalConnection;
		try {
			globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectGlobalCount(query, globalConnection, clusterName, clazz).longValue();

		return count;
	}

	@Override
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz) {
		return findGlobalByPkFromMaster(pk, clusterName, clazz, true);
	}

	@Override
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz, boolean useCache) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();
			return selectByPkWithCache(conn, clusterName, pk, clazz, useCache);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, Number... pks) {
		return findGlobalByPksFromMaster(clusterName, clazz, true, pks);
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, boolean useCache, Number... pks) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();
			return selectGlobalByPksWithCache(conn, clusterName, clazz, pks, useCache);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPkListFromMaster(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();
			return selectGlobalByPksWithCache(conn, clusterName, clazz, pks.toArray(new Number[pks.size()]), useCache);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public List<Map<String, Object>> findGlobalBySqlFromMaster(SQL sql, String clusterName) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			List<Map<String, Object>> result = selectGlobalBySql(conn, sql);

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz, boolean useCache) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			List<T> result = null;

			String tableName = ReflectUtil.getTableName(clazz);
			if (isSecondCacheAvailable(clazz, useCache)) {
				result = (List<T>) secondCache.getGlobal(query, clusterName, tableName);
			}

			if (result == null || result.isEmpty()) {
				if (isCacheAvailable(clazz, useCache)) {
					Number[] pkValues = selectGlobalPksByQuery(conn, query, clazz);
					result = selectGlobalByPksWithCache(conn, clusterName, clazz, pkValues, useCache);
				} else {
					result = selectGlobalByQuery(conn, query, clazz);
				}

				if (isSecondCacheAvailable(clazz, useCache)) {
					secondCache.putGlobal(query, clusterName, tableName, result);
				}
			}

			// 过滤从缓存结果, 将没有指定的字段设置为默认值.
			List<T> filteResult = new ArrayList<T>(result.size());
			if (query.hasQueryFields()) {
				for (T obj : result) {
					filteResult.add((T) ReflectUtil.cloneWithGivenField(obj, query.getFields()));
				}
				result = filteResult;
			}

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

}
