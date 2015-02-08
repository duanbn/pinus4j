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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.datalayer.IGlobalMasterQuery;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.transaction.ITransaction;
import org.pinus4j.utils.ReflectUtil;
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
		ITransaction tx = txManager.getTransaction();
		IDBResource dbResource = null;
		try {
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			if (tx != null) {
				tx.appendReadOnly(dbResource);
			}
			long count = selectGlobalCountWithCache(dbResource, clusterName, clazz, useCache).longValue();
			return count;
		} catch (Exception e) {
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public Number getGlobalCountFromMaster(IQuery query, String clusterName, Class<?> clazz) {
		ITransaction tx = txManager.getTransaction();
		IDBResource dbResource = null;
		try {
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			if (tx != null) {
				tx.appendReadOnly(dbResource);
			}
			long count = selectGlobalCount(query, dbResource, clusterName, clazz).longValue();

			return count;
		} catch (Exception e) {
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz) {
		return findGlobalByPkFromMaster(pk, clusterName, clazz, true);
	}

	@Override
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz, boolean useCache) {
		ITransaction tx = txManager.getTransaction();
		IDBResource dbResource = null;
		try {
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			if (tx != null) {
				tx.appendReadOnly(dbResource);
			}
			return selectByPkWithCache(dbResource, clusterName, pk, clazz, useCache);
		} catch (Exception e) {
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, Number... pks) {
		return findGlobalByPksFromMaster(clusterName, clazz, true, pks);
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, boolean useCache, Number... pks) {
		ITransaction tx = txManager.getTransaction();
		IDBResource dbResource = null;
		try {
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			if (tx != null) {
				tx.appendReadOnly(dbResource);
			}
			return selectGlobalByPksWithCache(dbResource, clusterName, clazz, pks, useCache);
		} catch (Exception e) {
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public <T> List<T> findGlobalByPkListFromMaster(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache) {
		ITransaction tx = txManager.getTransaction();
		IDBResource dbResource = null;
		try {
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			if (tx != null) {
				tx.appendReadOnly(dbResource);
			}
			return selectGlobalByPksWithCache(dbResource, clusterName, clazz, pks.toArray(new Number[pks.size()]),
					useCache);
		} catch (Exception e) {
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public List<Map<String, Object>> findGlobalBySqlFromMaster(SQL sql, String clusterName) {
		IDBResource next = null;
		for (String tableName : sql.getTableNames()) {
			IDBResource cur;
			try {
				cur = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
			} catch (DBClusterException e) {
				throw new DBOperationException(e);
			}
			if (next != null && (cur != next)) {
				throw new DBOperationException("the tables in sql maybe not at the same database");
			}
			next = cur;
		}

		ITransaction tx = txManager.getTransaction();
		try {
			if (tx != null) {
				tx.appendReadOnly(next);
			}
			List<Map<String, Object>> result = selectGlobalBySql(next, sql);
			return result;
		} catch (Exception e) {
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && next != null) {
				next.close();
			}
		}
	}

	@Override
	public <T> List<T> findGlobalByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz, boolean useCache) {
		ITransaction tx = txManager.getTransaction();
		IDBResource dbResource = null;
		try {
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			if (tx != null) {
				tx.appendReadOnly(dbResource);
			}

			List<T> result = null;

			String tableName = ReflectUtil.getTableName(clazz);
			if (isSecondCacheAvailable(clazz, useCache)) {
				result = (List<T>) secondCache.getGlobal(query, clusterName, tableName);
			}

			if (result == null || result.isEmpty()) {
				if (isCacheAvailable(clazz, useCache)) {
					Number[] pkValues = selectGlobalPksByQuery(dbResource, query, clazz);
					result = selectGlobalByPksWithCache(dbResource, clusterName, clazz, pkValues, useCache);
				} else {
					result = selectGlobalByQuery(dbResource, query, clazz);
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
			if (tx != null) {
				tx.rollback();
			}
			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}
}
