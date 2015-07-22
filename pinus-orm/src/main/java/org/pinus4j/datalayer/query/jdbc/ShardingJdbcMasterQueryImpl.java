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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.datalayer.query.IShardingMasterQuery;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分库分表查询实现.
 * replace by ShardingJdbcQueryImpl
 * 
 * @author duanbn
 */
@Deprecated
public class ShardingJdbcMasterQueryImpl extends AbstractJdbcQuery implements IShardingMasterQuery {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(ShardingJdbcMasterQueryImpl.class);

	@Override
	public Number getCountFromMaster(Class<?> clazz, boolean useCache) {
		Transaction tx = null;
		List<IDBResource> dbResources = null;
		try {
			tx = txManager.getTransaction();
			dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);
			long count = 0;
			for (IDBResource dbResource : dbResources) {
				if (tx != null) {
					tx.enlistResource((ShardingDBResource) dbResource);
				}
				count += selectCountWithCache((ShardingDBResource) dbResource, clazz, useCache).longValue();
			}
			return count;
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResources != null) {
				for (IDBResource dbResource : dbResources) {
					dbResource.close();
				}
			}
		}
	}

	@Override
	public Number getCountFromMaster(Class<?> clazz, IQuery query) {
		Transaction tx = null;
		List<IDBResource> dbResources = null;
		try {
			tx = txManager.getTransaction();
			dbResources = this.dbCluster.getAllMasterShardingDBResource(clazz);

			long count = 0;
			for (IDBResource dbResource : dbResources) {
				if (tx != null) {
					tx.enlistResource((ShardingDBResource) dbResource);
				}
				count += selectCount((ShardingDBResource) dbResource, clazz, query).longValue();
			}
			return count;
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResources != null) {
				for (IDBResource dbResource : dbResources) {
					dbResource.close();
				}
			}
		}

	}

	@Override
	public Number getCountFromMaster(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache) {
		Transaction tx = null;
		ShardingDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = _getDbFromMaster(clazz, shardingKey);
			if (tx != null) {
				tx.enlistResource(dbResource);
			}
			Number count = selectCountWithCache(dbResource, clazz, useCache);
			return count;
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public Number getCountFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz) {
		Transaction tx = null;
		ShardingDBResource dbResource = null;

		try {
			tx = txManager.getTransaction();
			dbResource = _getDbFromMaster(clazz, shardingKey);
			if (tx != null) {
				tx.enlistResource(dbResource);
			}
			return selectCount(dbResource, clazz, query);
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public <T> T findByPkFromMaster(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		Transaction tx = null;
		ShardingDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = _getDbFromMaster(clazz, shardingKey);
			if (tx != null) {
				tx.enlistResource(dbResource);
			}
			return selectByPkWithCache(dbResource, pk, clazz, useCache);
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
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
		Transaction tx = null;
		ShardingDBResource dbResource = null;

		try {
			tx = txManager.getTransaction();
			dbResource = _getDbFromMaster(clazz, shardingKey);
			if (tx != null) {
				tx.enlistResource(dbResource);
			}
			return selectByPksWithCache(dbResource, clazz, pks, useCache);
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public <T> List<T> findByPkListFromMaster(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache) {
		Transaction tx = null;
		ShardingDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = _getDbFromMaster(clazz, shardingKey);
			if (tx != null) {
				tx.enlistResource(dbResource);
			}
			return selectByPksWithCache(dbResource, clazz, pks.toArray(new Number[pks.size()]), useCache);
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	public List<Map<String, Object>> findBySqlFromMaster(SQL sql, IShardingKey<?> shardingKey) {
		ShardingDBResource next = null;
		for (String tableName : sql.getTableNames()) {
			ShardingDBResource cur = _getDbFromMaster(tableName, shardingKey);
			if (next != null && (cur != next)) {
				throw new DBOperationException("the tables in sql maybe not at the same database");
			}
			next = cur;
		}

		Transaction tx = null;
		try {
			tx = txManager.getTransaction();
			if (tx != null) {
				tx.enlistResource(next);
			}
			List<Map<String, Object>> result = selectBySql(next, sql);
			return result;
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && next != null) {
				next.close();
			}
		}
	}

	@Override
	public <T> List<T> findByQueryFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		Transaction tx = null;
		ShardingDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = _getDbFromMaster(clazz, shardingKey);
			if (tx != null) {
				tx.enlistResource(dbResource);
			}

			List<T> result = null;

			if (isSecondCacheAvailable(clazz, useCache)) {
				result = (List<T>) secondCache.get(query.getWhereSql(), dbResource);
			}

			if (result == null || result.isEmpty()) {
				if (isCacheAvailable(clazz, useCache)) {
					Number[] pkValues = selectPksByQuery(dbResource, query, clazz);
					result = selectByPksWithCache(dbResource, clazz, pkValues, useCache);
				} else {
					result = selectByQuery(dbResource, query, clazz);
				}

				if (isSecondCacheAvailable(clazz, useCache)) {
					secondCache.put(query.getWhereSql(), dbResource, result);
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
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	/**
	 * 路由选择.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param shardingKey
	 *            路由因子
	 */
	private ShardingDBResource _getDbFromMaster(Class<?> clazz, IShardingKey<?> shardingKey) {
		String tableName = ReflectUtil.getTableName(clazz);
		return _getDbFromMaster(tableName, shardingKey);
	}

	private ShardingDBResource _getDbFromMaster(String tableName, IShardingKey<?> shardingKey) {
		ShardingDBResource shardingDBResource = null;
		try {
			shardingDBResource = (ShardingDBResource) this.dbCluster.selectDBResourceFromMaster(tableName, shardingKey);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + shardingDBResource + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return shardingDBResource;
	}

}
