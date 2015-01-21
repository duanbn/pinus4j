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
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.api.query.IQuery;
import org.pinus.cluster.beans.DBInfo;
import org.pinus.datalayer.IGlobalSlaveQuery;
import org.pinus.datalayer.SQLBuilder;
import org.pinus.exception.DBClusterException;
import org.pinus.exception.DBOperationException;
import org.pinus.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * global slave database query implements.
 *
 * @author duanbn
 * @since 0.7.1
 */
public class GlobalJdbcSlaveQueryImpl extends AbstractJdbcQuery implements IGlobalSlaveQuery {

    /**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(GlobalJdbcSlaveQueryImpl.class);

    @Override
	public <T> T findGlobalOneByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave) {
		List<T> entities = findGlobalByQueryFromSlave(query, clusterName, clazz, useCache, slave);

		if (entities.isEmpty()) {
			return null;
		}

		return entities.get(0);
	}

	@Override
	public Number getGlobalCountFromSlave(String clusterName, Class<?> clazz, boolean useCache, EnumDBMasterSlave slave) {
		DBInfo dbConnInfo = null;
		try {
			dbConnInfo = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectGlobalCountWithCache(dbConnInfo, clusterName, clazz, useCache).longValue();

		return count;
	}

	@Override
	public Number getGlobalCountFromSlave(IQuery query, String clusterName, Class<?> clazz, EnumDBMasterSlave slave) {
		DBInfo globalConnection;
		try {
			globalConnection = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectGlobalCount(query, globalConnection, clusterName, clazz).longValue();

		return count;
	}

	@Override
	public <T> T findGlobalByPkFromSlave(Number pk, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			conn = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave).getDatasource().getConnection();
			return selectByPkWithCache(conn, clusterName, pk, clazz, useCache);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromSlave(String clusterName, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks) {
		return findGlobalByPksFromSlave(clusterName, clazz, slave, true, pks);
	}

	@Override
	public <T> List<T> findGlobalByPksFromSlave(String clusterName, Class<T> clazz, EnumDBMasterSlave slave,
			boolean useCache, Number... pks) {

		List<T> result = new ArrayList<T>();

		Connection conn = null;
		try {
			DBInfo globalConnection = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);

			conn = globalConnection.getDatasource().getConnection();

			result.addAll(selectGlobalByPksWithCache(conn, clusterName, clazz, pks, useCache));
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return result;

	}

	@Override
	public <T> List<T> findGlobalByPkListFromSlave(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave slave) {
		List<T> result = new ArrayList<T>();

		Connection conn = null;
		try {
			DBInfo globalConnection = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);

			conn = globalConnection.getDatasource().getConnection();

			result.addAll(selectGlobalByPksWithCache(conn, clusterName, clazz, pks.toArray(new Number[pks.size()]),
					useCache));
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> findGlobalBySqlFromSlave(SQL sql, String clusterName, EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			DBInfo slaveGlobal = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);

			conn = slaveGlobal.getDatasource().getConnection();

			List<Map<String, Object>> result = selectGlobalBySql(conn, sql);

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			DBInfo globalConnection = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);
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
