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
import javax.transaction.xa.XAResource;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * global query implements.
 *
 * @author duanbn.
 * @since 1.1.1
 */
public class GlobalJdbcQueryImpl extends AbstractJdbcQuery implements IGlobalQuery {

	public static final Logger LOG = LoggerFactory.getLogger(GlobalJdbcQueryImpl.class);

	@Override
	public Number getCount(Class<?> clazz) {
		return getCount(clazz, true);
	}

	@Override
	public Number getCount(Class<?> clazz, boolean useCache) {
		return getCount(clazz, useCache, EnumDBMasterSlave.MASTER);
	}

	@Override
	public Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();

			// select db resource.
			if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
			} else {
				dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
			}

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			}

			long count = selectGlobalCountWithCache(dbResource, clusterName, clazz, useCache).longValue();
			if (count == 0) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
				count = selectGlobalCountWithCache(dbResource, clusterName, clazz, useCache).longValue();
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
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}

	}

	@Override
	public Number getCountByQuery(IQuery query, Class<?> clazz) {
		return getCountByQuery(query, clazz, true);
	}

	@Override
	public Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache) {
		return getCountByQuery(query, clazz, useCache, EnumDBMasterSlave.MASTER);
	}

	@Override
	public Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();

			if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
			} else {
				dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
			}

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			}

			long count = selectGlobalCount(query, dbResource, clusterName, clazz).longValue();
			if (count == 0) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
				count = selectGlobalCount(query, dbResource, clusterName, clazz).longValue();
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
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	@Override
	public <T> T getByPk(Number pk, Class<T> clazz) {
		return getByPk(pk, clazz, true);
	}

	@Override
	public <T> T getByPk(Number pk, Class<T> clazz, boolean useCache) {
		return getByPk(pk, clazz, useCache, EnumDBMasterSlave.MASTER);
	}

	@Override
	public <T> T getByPk(Number pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {

			tx = txManager.getTransaction();

			if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
			} else {
				dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
			}

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			}

			T data = selectByPkWithCache(dbResource, clusterName, pk, clazz, useCache);
			if (data == null) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
				data = selectByPkWithCache(dbResource, clusterName, pk, clazz, useCache);
			}

			return data;
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
	public <T> List<T> findByPkList(List<? extends Number> pks, Class<T> clazz) {
		return findByPkList(pks, clazz, true);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, Class<T> clazz, boolean useCache) {
		return findByPkList(pks, clazz, useCache, EnumDBMasterSlave.MASTER);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {

			tx = txManager.getTransaction();

			if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
			} else {
				dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
			}

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			}

			List<T> data = selectGlobalByPksWithCache(dbResource, clusterName, clazz,
					pks.toArray(new Number[pks.size()]), useCache);
			if (data.isEmpty()) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
				data = selectGlobalByPksWithCache(dbResource, clusterName, clazz, pks.toArray(new Number[pks.size()]),
						useCache);
			}

			return data;
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
	public <T> T findOneByQuery(IQuery query, Class<T> clazz) {
		return findOneByQuery(query, clazz, true);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache) {
		return findOneByQuery(query, clazz, useCache, EnumDBMasterSlave.MASTER);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
		List<T> entities = findByQuery(query, clazz, useCache, masterSlave);

		if (entities.isEmpty()) {
			return null;
		}

		return entities.get(0);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, Class<T> clazz) {
		return findByQuery(query, clazz, true);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache) {
		return findByQuery(query, clazz, useCache, EnumDBMasterSlave.MASTER);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();

			if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
				dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
			} else {
				dbResource = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
			}

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			}

			List<T> result = null;

			if (isSecondCacheAvailable(clazz, useCache)) {
				result = (List<T>) secondCache.getGlobal(query.getWhereSql(), clusterName, tableName);
			}

			if (result == null || result.isEmpty()) {
				if (isCacheAvailable(clazz, useCache)) {
					Number[] pkValues = selectGlobalPksByQuery(dbResource, query, clazz);
					result = selectGlobalByPksWithCache(dbResource, clusterName, clazz, pkValues, useCache);

					if (result == null) {
						dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
						result = selectGlobalByPksWithCache(dbResource, clusterName, clazz, pkValues, useCache);
					}
				} else {
					result = selectGlobalByQuery(dbResource, query, clazz);
					if (result == null) {
						dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
						result = selectGlobalByQuery(dbResource, query, clazz);
					}
				}

				if (isSecondCacheAvailable(clazz, useCache)) {
					secondCache.putGlobal(query.getWhereSql(), clusterName, tableName, result);
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
	public List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz) {
		return findBySql(sql, clazz, EnumDBMasterSlave.MASTER);
	}

	@Override
	public List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz, EnumDBMasterSlave masterSlave) {
		String clusterName = ReflectUtil.getClusterName(clazz);

		IDBResource next = null;

		for (String tableName : sql.getTableNames()) {
			IDBResource cur = null;

			try {
				if (EnumDBMasterSlave.MASTER == masterSlave || !this.dbCluster.isGlobalSlaveExist(clusterName)) {
					cur = this.dbCluster.getMasterGlobalDBResource(clusterName, tableName);
				} else {
					cur = this.dbCluster.getSlaveGlobalDBResource(clusterName, tableName, masterSlave);
				}
			} catch (DBClusterException e) {
				throw new DBOperationException(e);
			}

			if (next != null && (cur != next)) {
				throw new DBOperationException("the tables in sql maybe not at the same database");
			}

			next = cur;
		}

		Transaction tx = null;
		try {

			tx = txManager.getTransaction();

			if (tx != null) {
				tx.enlistResource((XAResource) next);
			}

			List<Map<String, Object>> result = selectGlobalBySql(next, sql);

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

}
