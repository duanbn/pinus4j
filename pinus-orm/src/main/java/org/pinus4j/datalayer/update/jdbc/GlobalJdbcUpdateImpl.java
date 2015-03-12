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

package org.pinus4j.datalayer.update.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.update.IGlobalUpdate;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalJdbcUpdateImpl extends AbstractJdbcUpdate implements IGlobalUpdate {

	public static final Logger LOG = LoggerFactory.getLogger(GlobalJdbcUpdateImpl.class);

	@Override
	public Number globalSave(Object entity, String clusterName) {
		List<Object> entities = new ArrayList<Object>(1);
		entities.add(entity);
		return globalSaveBatch(entities, clusterName)[0];
	}

	@Override
	public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName) {
		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		int entitySize = entities.size();
		Number[] pks = new Number[entitySize];
		boolean isCheckPrimaryKey = true;

		// 检查插入数据的主键
		Map<Number, Object> map = new LinkedHashMap<Number, Object>(entitySize);
		Number pk = null, maxPk = 0;
		Object entity = null;
		for (int i = 0; i < entitySize; i++) {
			entity = entities.get(i);
			pk = ReflectUtil.getPkValue(entity);
			if (pk == null || pk.intValue() == 0) {
				map.put(i, entity);
			} else {
				pks[i] = pk;
				maxPk = pk.intValue() > maxPk.intValue() ? pk : maxPk;
			}
		}
		// 如果主键为0，则设置主键
		if (!map.isEmpty()) {
			isCheckPrimaryKey = false;
			int[] newPks = this.idGenerator.genClusterUniqueIntIdBatch(Const.ZK_PRIMARYKEY + "/" + clusterName,
					tableName, map.size(), maxPk.longValue());
			int i = 0;
			for (Map.Entry<Number, Object> entry : map.entrySet()) {
				int pos = entry.getKey().intValue();
				try {
					ReflectUtil.setPkValue(entities.get(pos), newPks[i]);
				} catch (Exception e) {
					throw new DBOperationException(e);
				}
				pks[pos] = newPks[i];
				i++;
			}
		}

		if (isCheckPrimaryKey)
			this.idGenerator.checkAndSetPrimaryKey(maxPk.longValue(), clusterName, tableName);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));
			Connection conn = dbResource.getConnection();

			_saveBatchGlobal(conn, entities);

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			} else {
				dbResource.commit();
			}

			if (isCacheAvailable(clazz)) {
				primaryCache.incrCountGlobal(clusterName, tableName, entities.size());
			}

			if (isSecondCacheAvailable(clazz)) {
				secondCache.removeGlobal(clusterName, tableName);
			}
		} catch (Exception e1) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e) {
					throw new DBOperationException(e);
				}
			} else {
				if (dbResource != null) {
					dbResource.rollback();
				}
			}

			throw new DBOperationException(e1);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}

		return pks;
	}

	@Override
	public void globalUpdate(Object entity, String clusterName) {
		List<Object> entities = new ArrayList<Object>();
		entities.add(entity);
		globalUpdateBatch(entities, clusterName);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void globalUpdateBatch(List<? extends Object> entities, String clusterName) {
		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		Transaction tx = null;
		IDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));

			Connection conn = dbResource.getConnection();

			_updateBatchGlobal(conn, entities);

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			} else {
				dbResource.commit();
			}

			// 删除缓存
			if (isCacheAvailable(clazz)) {
				List pks = new ArrayList(entities.size());
				for (Object entity : entities) {
					pks.add((Number) ReflectUtil.getPkValue(entity));
				}
				primaryCache.removeGlobal(clusterName, tableName, pks);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.removeGlobal(clusterName, tableName);
			}
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			} else {
				if (dbResource != null) {
					dbResource.rollback();
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
	public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName) {
		List<Number> pks = new ArrayList<Number>(1);
		pks.add(pk);
		globalRemoveByPks(pks, clazz, clusterName);
	}

	@Override
	public void globalRemoveByPks(List<? extends Number> pks, Class<?> clazz, String clusterName) {

		Transaction tx = null;
		IDBResource dbResource = null;
		try {
			tx = txManager.getTransaction();
			dbResource = this.dbCluster.getMasterGlobalDBResource(clusterName, ReflectUtil.getTableName(clazz));

			Connection conn = dbResource.getConnection();

			_removeByPksGlobal(conn, pks, clazz);

			if (tx != null) {
				tx.enlistResource((XAResource) dbResource);
			} else {
				dbResource.commit();
			}

			// 删除缓存
			String tableName = ReflectUtil.getTableName(clazz);
			if (isCacheAvailable(clazz)) {
				primaryCache.removeGlobal(clusterName, tableName, pks);
				primaryCache.decrCountGlobal(clusterName, tableName, pks.size());
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.removeGlobal(clusterName, tableName);
			}
		} catch (Exception e) {
			if (tx != null) {
				try {
					tx.rollback();
				} catch (Exception e1) {
					throw new DBOperationException(e1);
				}
			} else {
				if (dbResource != null) {
					dbResource.rollback();
				}
			}

			throw new DBOperationException(e);
		} finally {
			if (tx == null && dbResource != null) {
				dbResource.close();
			}
		}
	}

	private void _saveBatchGlobal(Connection conn, List<? extends Object> entities) {
		_saveBatch(conn, entities, -1);
	}

	private void _updateBatchGlobal(Connection conn, List<? extends Object> entities) {
		_updateBatch(conn, entities, -1);
	}

	private void _removeByPksGlobal(Connection conn, List<? extends Number> pks, Class<?> clazz) {
		_removeByPks(conn, pks, clazz, -1);
	}

}
