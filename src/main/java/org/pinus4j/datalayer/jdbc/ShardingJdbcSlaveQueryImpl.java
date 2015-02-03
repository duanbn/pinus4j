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

package org.pinus4j.datalayer.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinus4j.api.IShardingKey;
import org.pinus4j.api.SQL;
import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.DB;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.datalayer.IShardingSlaveQuery;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从库查询实现.
 * 
 * @author duanbn
 * 
 */
public class ShardingJdbcSlaveQueryImpl extends AbstractJdbcQuery implements IShardingSlaveQuery {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(ShardingJdbcSlaveQueryImpl.class);

	@Override
	public Number getCountFromSlave(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache,
			EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(clazz, shardingKey, slave);

		return selectCountWithCache(db, clazz, useCache);
	}

	@Override
	public <T> T findByPkFromSlave(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(clazz, shardingKey, slave);

		return selectByPkWithCache(db, pk, clazz, useCache);
	}

	@Override
	public <T> T findOneByQueryFromSlave(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave) {
		List<T> entities = findByQueryFromSlave(query, shardingKey, clazz, useCache, slave);

		if (entities.isEmpty()) {
			return null;
		}

		return entities.get(0);
	}

	@Override
	public <T> List<T> findByPksFromSlave(IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks) {
		return findByPksFromSlave(shardingKey, clazz, slave, true, pks);
	}

	@Override
	public <T> List<T> findByPksFromSlave(IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave slave,
			boolean useCache, Number... pks) {
		DB db = _getDbFromSlave(clazz, shardingKey, slave);

		return selectByPksWithCache(db, clazz, pks, true);
	}

	@Override
	public <T> List<T> findByPkListFromSlave(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(clazz, shardingKey, slave);

		return selectByPksWithCache(db, clazz, pks.toArray(new Number[pks.size()]), useCache);
	}

	@Deprecated
	@Override
	public <T> List<T> findByShardingPairFromSlave(List<IShardingKey<?>> shardingValues, Class<T> clazz,
			EnumDBMasterSlave slave, Number... pks) {
		if (shardingValues.size() != pks.length) {
			throw new DBOperationException("分库分表列表和主键数量不等");
		}

		List<T> result = new ArrayList<T>(pks.length);
		IShardingKey<?> shardingKey = null;
		Number pk = null;
		DB db = null;
		T data = null;
		for (int i = 0; i < pks.length; i++) {
			shardingKey = shardingValues.get(i);
			pk = pks[i];
			db = _getDbFromSlave(clazz, shardingKey, slave);

			data = selectByPkWithCache(db, pk, clazz, true);
			if (data != null) {
				result.add(data);
			}
		}

		return result;
	}

	@Override
	public <T> List<T> findByShardingPairFromSlave(List<? extends Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz, boolean useCache, EnumDBMasterSlave slave) {
		if (shardingValues.size() != pks.size()) {
			throw new DBOperationException("分库分表列表和主键数量不等");
		}

		List<T> result = new ArrayList<T>(pks.size());
		IShardingKey<?> shardingKey = null;
		Number pk = null;
		DB db = null;
		T data = null;
		for (int i = 0; i < pks.size(); i++) {
			shardingKey = shardingValues.get(i);
			pk = pks.get(i);
			db = _getDbFromSlave(clazz, shardingKey, slave);

			data = selectByPkWithCache(db, pk, clazz, useCache);
			if (data != null) {
				result.add(data);
			}
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> findBySqlFromSlave(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave slave) {
		DB next = null;
		for (String tableName : sql.getTableNames()) {
			DB cur = _getDbFromSlave(tableName, shardingKey, slave);
			if (next != null && (cur != next)) {
				throw new DBOperationException("the tables in sql maybe not at the same database");
			}
			next = cur;
		}

		List<Map<String, Object>> result = selectBySql(next, sql);

		return result;
	}

	@Override
	public <T> List<T> findByQueryFromSlave(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(clazz, shardingKey, slave);

		List<T> result = null;

		if (isSecondCacheAvailable(clazz, useCache)) {
			result = (List<T>) secondCache.get(query, db);
		}

		if (result == null || result.isEmpty()) {
			if (isCacheAvailable(clazz, useCache)) {
				Number[] pkValues = selectPksByQuery(db, query, clazz);
				result = selectByPksWithCache(db, clazz, pkValues, useCache);
			} else {
				result = selectByQuery(db, query, clazz);
			}

			if (isSecondCacheAvailable(clazz, useCache)) {
				secondCache.put(query, db, result);
			}
		}

		// 过滤从缓存结果, 将没有指定的字段设置为默认值.
		List<T> filteResult = new ArrayList<T>(result.size());
		if (query.hasQueryFields()) {
			for (T obj : result) {
				try {
					filteResult.add((T) ReflectUtil.cloneWithGivenField(obj, query.getFields()));
				} catch (Exception e) {
					throw new DBOperationException(e);
				}
			}
			result = filteResult;
		}

		return result;
	}

	/**
	 * 路由选择.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param shardingKey
	 *            路由因子
	 */
	private DB _getDbFromSlave(Class<?> clazz, IShardingKey<?> shardingKey, EnumDBMasterSlave slave) {
		String tableName = ReflectUtil.getTableName(clazz);
		DB db = null;
		try {
			db = this.dbCluster.selectDbFromSlave(tableName, shardingKey, slave);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + db + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return db;
	}

	private DB _getDbFromSlave(String tableName, IShardingKey<?> shardingKey, EnumDBMasterSlave slave) {
		DB db = null;
		try {
			db = this.dbCluster.selectDbFromSlave(tableName, shardingKey, slave);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + db + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return db;
	}

}
