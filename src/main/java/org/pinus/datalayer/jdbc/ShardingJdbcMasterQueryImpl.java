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

import org.pinus.api.IShardingKey;
import org.pinus.api.SQL;
import org.pinus.api.query.IQuery;
import org.pinus.cluster.DB;
import org.pinus.cluster.IDBCluster;
import org.pinus.cluster.beans.DBConnectionInfo;
import org.pinus.datalayer.IShardingMasterQuery;
import org.pinus.datalayer.SQLBuilder;
import org.pinus.exception.DBClusterException;
import org.pinus.exception.DBOperationException;
import org.pinus.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分库分表查询实现.
 * 
 * @author duanbn
 */
public class ShardingJdbcMasterQueryImpl extends AbstractJdbcQuery implements IShardingMasterQuery {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(ShardingJdbcMasterQueryImpl.class);

	@Override
	public Number getCountFromMaster(Class<?> clazz, boolean useCache) {
		List<DB> dbs = this.dbCluster.getAllMasterShardingDB(clazz);
		long count = 0;
		for (DB db : dbs) {
			count += selectCountWithCache(db, clazz, useCache).longValue();
		}
		return count;
	}

	@Override
	public Number getCountFromMaster(Class<?> clazz, IQuery query) {
		List<DB> dbs = this.dbCluster.getAllMasterShardingDB(clazz);
		long count = 0;
		for (DB db : dbs) {
			count += selectCount(db, clazz, query).longValue();
		}
		return count;
	}

	@Override
	public Number getCountFromMaster(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectCountWithCache(db, clazz, useCache);
	}

	@Override
	public Number getCountFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectCount(db, clazz, query);
	}

	@Override
	public <T> T findByPkFromMaster(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectByPkWithCache(db, pk, clazz, useCache);
	}

	@Override
	public <T> T findOneByQueryFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		List<T> entities = findByQueryFromMaster(query, shardingKey, clazz, useCache);

		if (entities.isEmpty()) {
			return null;
		}

		if (entities.size() > 1) {
			throw new DBOperationException("查询结果大于1条记录");
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
	public <T> List<T> findByPksFromMaster(IShardingKey<?> shardingKey, Class<T> clazz, Number... pks) {
		return findByPksFromMaster(shardingKey, clazz, true, pks);
	}

	@Override
	public <T> List<T> findByPksFromMaster(IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache, Number... pks) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectByPksWithCache(db, clazz, pks, useCache);
	}

	@Override
	public <T> List<T> findByPkListFromMaster(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectByPksWithCache(db, clazz, pks.toArray(new Number[pks.size()]), useCache);
	}

	@Deprecated
	@Override
	public <T> List<T> findByShardingPairFromMaster(List<IShardingKey<?>> shardingValues, Class<T> clazz, Number... pks) {
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
			db = _getDbFromMaster(clazz, shardingKey);

			data = selectByPkWithCache(db, pk, clazz, true);
			if (data != null) {
				result.add(data);
			}
		}

		return result;
	}

	@Override
	public <T> List<T> findByShardingPairFromMaster(List<? extends Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz, boolean useCache) {
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
			db = _getDbFromMaster(clazz, shardingKey);

			data = selectByPkWithCache(db, pk, clazz, useCache);
			if (data != null) {
				result.add(data);
			}
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> findBySqlFromMaster(SQL sql, IShardingKey<?> shardingKey) {
		DB next = null;
		for (String tableName : sql.getTableNames()) {
			DB cur = _getDbFromMaster(tableName, shardingKey);
			if (next != null && (cur != next)) {
				throw new DBOperationException("the tables in sql maybe not at the same database");
			}
			next = cur;
		}

		List<Map<String, Object>> result = selectBySql(next, sql);

		return result;
	}

	@Override
	public <T> List<T> findByQueryFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		DB db = _getDbFromMaster(clazz, shardingKey);

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
	private DB _getDbFromMaster(Class<?> clazz, IShardingKey<?> shardingKey) {
		String tableName = ReflectUtil.getTableName(clazz);
		DB db = null;
		try {
			db = this.dbCluster.selectDbFromMaster(tableName, shardingKey);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + db + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return db;
	}

	private DB _getDbFromMaster(String tableName, IShardingKey<?> shardingKey) {
		DB db = null;
		try {
			db = this.dbCluster.selectDbFromMaster(tableName, shardingKey);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + db + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return db;
	}

}
