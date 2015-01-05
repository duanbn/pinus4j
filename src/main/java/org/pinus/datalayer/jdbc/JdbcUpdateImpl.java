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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pinus.api.IShardingKey;
import org.pinus.cache.IPrimaryCache;
import org.pinus.cache.ISecondCache;
import org.pinus.cluster.DB;
import org.pinus.cluster.IDBCluster;
import org.pinus.cluster.beans.DBConnectionInfo;
import org.pinus.constant.Const;
import org.pinus.datalayer.IShardingUpdate;
import org.pinus.datalayer.SQLBuilder;
import org.pinus.exception.DBClusterException;
import org.pinus.exception.DBOperationException;
import org.pinus.generator.IIdGenerator;
import org.pinus.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分库分表更新实现. 更新操作包括插入、删除、更新，这些操作只操作主库.
 * 
 * @author duanbn
 */
public class JdbcUpdateImpl implements IShardingUpdate {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(JdbcUpdateImpl.class);

	/**
	 * 数据库集群引用
	 */
	private IDBCluster dbCluster;

	/**
	 * ID生成器
	 */
	private IIdGenerator idGenerator;

	/**
	 * 一级缓存引用.
	 */
	private IPrimaryCache primaryCache;

	/**
	 * 二级缓存引用.
	 */
	private ISecondCache secondCache;

	@Override
	public void setIdGenerator(IIdGenerator idGenerator) {
		this.idGenerator = idGenerator;
	}

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
		Map<Number, Object> map = new HashMap<Number, Object>(entitySize);
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

		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			_saveBatchGlobal(conn, entities);

			if (isCacheAvailable(clazz)) {
				primaryCache.incrCountGlobal(clusterName, tableName, entities.size());
			}

			if (isSecondCacheAvailable(clazz)) {
				secondCache.removeGlobal(clusterName, tableName);
			}
		} catch (Exception e1) {
			throw new DBOperationException(e1);
		} finally {
			SQLBuilder.close(conn);
		}

		return pks;
	}

	@Override
	public void globalUpdate(Object entity, String clusterName) {
		List<Object> entities = new ArrayList<Object>();
		entities.add(entity);
		globalUpdateBatch(entities, clusterName);
	}

	@Override
	public void globalUpdateBatch(List<? extends Object> entities, String clusterName) {
		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			_updateBatchGlobal(conn, entities);

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
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
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

		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			_removeByPksGlobal(conn, pks, clazz);

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
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Number save(Object entity, IShardingKey shardingKey) {
		Class clazz = entity.getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		boolean isCheckPrimaryKey = true;
		// set primary key.
		Number pk = ReflectUtil.getPkValue(entity);
		if (pk == null || pk.intValue() == 0) {
			isCheckPrimaryKey = false;
			pk = this.idGenerator.genClusterUniqueLongId(Const.ZK_PRIMARYKEY + "/" + shardingKey.getClusterName(),
					tableName);
			try {
				ReflectUtil.setPkValue(entity, pk);
			} catch (Exception e) {
				throw new DBOperationException(e);
			}
		}

		if (isCheckPrimaryKey)
			this.idGenerator.checkAndSetPrimaryKey(pk.longValue(), shardingKey.getClusterName(), tableName);

		DB db = _getDbFromMaster(tableName, shardingKey);

		List<Object> entities = new ArrayList<Object>(1);
		entities.add(entity);
		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_saveBatch(conn, entities, db.getTableIndex());

			if (isCacheAvailable(clazz)) {
				primaryCache.incrCount(db, 1);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return pk;
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		DB db = _getDbFromMaster(tableName, shardingKey);

		int entitySize = entities.size();
		Number[] pks = new Number[entitySize];
		boolean isCheckPrimaryKey = true;

		// 如果主键为0，则设置主键
		Map<Number, Object> map = new HashMap<Number, Object>(entitySize);
		Number pk = null, maxPk = 0;
		Object entity = null;
		for (int i = 0; i < entitySize; i++) {
			entity = entities.get(i);
			pk = ReflectUtil.getPkValue(entity);
			if (pk == null || pk.longValue() == 0) {
				map.put(i, entity);
			} else {
				pks[i] = pk;
				maxPk = pk.intValue() > maxPk.intValue() ? pk : maxPk;
			}
		}
		if (!map.isEmpty()) {
			isCheckPrimaryKey = false;
			int[] newPks = this.idGenerator.genClusterUniqueIntIdBatch(Const.ZK_PRIMARYKEY + "/" + db.getClusterName(),
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
			this.idGenerator.checkAndSetPrimaryKey(maxPk.longValue(), db.getClusterName(), tableName);

		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_saveBatch(conn, entities, db.getTableIndex());

			if (isCacheAvailable(clazz)) {
				primaryCache.incrCount(db, pks.length);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return pks;
	}

	@Override
	public void update(Object entity, IShardingKey<?> shardingKey) {
		List<Object> entities = new ArrayList<Object>();
		entities.add(entity);
		updateBatch(entities, shardingKey);
	}

	@Override
	public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		Class<?> clazz = entities.get(0).getClass();

		String talbeName = ReflectUtil.getTableName(clazz);
		DB db = _getDbFromMaster(talbeName, shardingKey);
		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_updateBatch(conn, entities, db.getTableIndex());

			// 清理缓存
			if (isCacheAvailable(clazz)) {
				List pks = new ArrayList(entities.size());
				for (Object entity : entities) {
					pks.add((Number) ReflectUtil.getPkValue(entity));
				}
				primaryCache.remove(db, pks);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

	}

	@Override
	public void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz) {
		List<Number> pks = new ArrayList<Number>(1);
		pks.add(pk);
		removeByPks(pks, shardingKey, clazz);
	}

	@Override
	public void removeByPks(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<?> clazz) {
		String talbeName = ReflectUtil.getTableName(clazz);
		DB db = _getDbFromMaster(talbeName, shardingKey);

		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_removeByPks(conn, pks, clazz, db.getTableIndex());

			// 删除缓存
			if (isCacheAvailable(clazz)) {
				primaryCache.remove(db, pks);
				primaryCache.decrCount(db, pks.size());
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

	}

	@Override
	public void setDBCluster(IDBCluster dbCluster) {
		this.dbCluster = dbCluster;
	}

	@Override
	public void setPrimaryCache(IPrimaryCache primaryCache) {
		this.primaryCache = primaryCache;
	}

	@Override
	public void setSecondCache(ISecondCache secondCache) {
		this.secondCache = secondCache;
	}

	/**
	 * 路由选择.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param shardingKey
	 *            路由因子
	 */
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

	private void _saveBatchGlobal(Connection conn, List<? extends Object> entities) {
		_saveBatch(conn, entities, -1);
	}

	/**
	 * 执行保存数据操作.
	 *
	 * @param conn
	 *            数据库连接
	 * @param entities
	 *            需要被保存的数据
	 * @param tableIndex
	 *            分片表下标. 当-1时忽略下标
	 */
	private void _saveBatch(Connection conn, List<? extends Object> entities, int tableIndex) {
		Statement st = null;
		try {
			conn.setAutoCommit(false);

			st = SQLBuilder.getInsert(conn, entities, tableIndex);
			st.executeBatch();

			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				LOG.error(e1.getMessage());
			}
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(st);
		}
	}

	private void _updateBatchGlobal(Connection conn, List<? extends Object> entities) {
		_updateBatch(conn, entities, -1);
	}

	private void _updateBatch(Connection conn, List<? extends Object> entities, int tableIndex) {
		Statement st = null;
		try {
			conn.setAutoCommit(false);

			st = SQLBuilder.getUpdate(conn, entities, tableIndex);
			st.executeBatch();
			conn.commit();

		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				LOG.error(e1.getMessage());
			}
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(st);
		}
	}

	private void _removeByPksGlobal(Connection conn, List<? extends Number> pks, Class<?> clazz) {
		_removeByPks(conn, pks, clazz, -1);
	}

	private void _removeByPks(Connection conn, List<? extends Number> pks, Class<?> clazz, int tableIndex) {
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(SQLBuilder.buildDeleteByPks(clazz, tableIndex, pks));
			ps.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				LOG.error(e1.getMessage());
			}
		} finally {
			SQLBuilder.close(null, ps);
		}
	}

	/**
	 * 判断一级缓存是否可用
	 * 
	 * @return true:启用cache, false:不启用
	 */
	protected boolean isCacheAvailable(Class<?> clazz) {
		return primaryCache != null && ReflectUtil.isCache(clazz);
	}

	/**
	 * 判断二级缓存是否可用
	 * 
	 * @return true:启用cache, false:不启用
	 */
	protected boolean isSecondCacheAvailable(Class<?> clazz) {
		return secondCache != null && ReflectUtil.isCache(clazz);
	}

}
