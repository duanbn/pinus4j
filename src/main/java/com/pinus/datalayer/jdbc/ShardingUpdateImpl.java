package com.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pinus.api.IShardingKey;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.datalayer.IShardingUpdate;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBOperationException;
import com.pinus.generator.IIdGenerator;
import com.pinus.util.ReflectUtil;

/**
 * 分库分表更新实现. 更新操作包括插入、删除、更新，这些操作只操作主库.
 * 
 * @author duanbn
 */
public class ShardingUpdateImpl implements IShardingUpdate {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(ShardingUpdateImpl.class);

	/**
	 * 数据库集群引用
	 */
	private IDBCluster dbCluster;

	/**
	 * ID生成器
	 */
	private IIdGenerator idGenerator;

	/**
	 * 主缓存引用.
	 */
	private IPrimaryCache primaryCache;

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

		// 生成主键
		int[] newPks = this.idGenerator.genClusterUniqueIntIdBatch(dbCluster, clusterName, tableName, entities.size());

		Number[] pks = new Number[newPks.length];
		for (int i = 0; i < entities.size(); i++) {
			try {
				ReflectUtil.setPkValue(entities.get(i), newPks[i]);
			} catch (Exception e) {
				throw new DBOperationException(e);
			}
			pks[i] = newPks[i];
		}

		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);
			conn = globalConnection.getDatasource().getConnection();
			_saveBatchGlobal(conn, entities);

			if (primaryCache != null) {
				primaryCache.putGlobal(clusterName, tableName, entities);
				primaryCache.incrCountGlobal(clusterName, tableName, entities.size());
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

			// 更新缓存
			if (primaryCache != null) {
				primaryCache.putGlobal(clusterName, tableName, entities);
			}
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName) {
		globalRemoveByPks(new Number[] { pk }, clazz, clusterName);
	}

	@Override
	public void globalRemoveByPks(Number[] pks, Class<?> clazz, String clusterName) {

		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			_removeByPksGlobal(conn, pks, clazz);

			// 删除缓存
			if (primaryCache != null) {
				String tableName = ReflectUtil.getTableName(clazz);
				primaryCache.removeGlobal(clusterName, tableName, pks);
				primaryCache.decrCountGlobal(clusterName, tableName, pks.length);
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
		String tableName = ReflectUtil.getTableName(entity.getClass());

		long pk = this.idGenerator.genClusterUniqueLongId(dbCluster, shardingKey.getClusterName(), tableName);
		try {
			ReflectUtil.setPkValue(entity, pk);
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
		if (shardingKey.getValue() instanceof Number) {
			if (shardingKey.getValue() == null || ((Number) shardingKey.getValue()).intValue() == 0) {
				shardingKey.setValue(pk);
			}
		} else if (shardingKey.getValue() instanceof String) {
			if (shardingKey.getValue() == null) {
				throw new DBOperationException("使用String做Sharding时，ShardingKey的值不能为Null");
			}
		} else {
			throw new DBOperationException("不支持的ShardingKey类型, 只支持Number或String");
		}

		DB db = _getDbFromMaster(tableName, shardingKey);

		List<Object> entities = new ArrayList<Object>(1);
		entities.add(entity);
		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_saveBatch(conn, entities, db.getTableIndex());
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		if (primaryCache != null) {
			primaryCache.put(db, pk, entity);
			primaryCache.incrCount(db, 1);
		}

		return pk;
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		if (shardingKey.getValue() instanceof Number && ((Number) shardingKey.getValue()).intValue() == 0) {
			throw new DBOperationException("分库分表因子的值不能为0, shardingKey=" + shardingKey);
		}

		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		DB db = _getDbFromMaster(tableName, shardingKey);

		// 生成主键
		int[] newPks = this.idGenerator.genClusterUniqueIntIdBatch(dbCluster, db.getClusterName(), tableName,
				entities.size());

		Number[] pks = new Number[newPks.length];
		Connection conn = null;
		try {
			for (int i = 0; i < entities.size(); i++) {
				ReflectUtil.setPkValue(entities.get(i), newPks[i]);
				pks[i] = newPks[i];
			}

			conn = db.getDatasource().getConnection();

			_saveBatch(conn, entities, db.getTableIndex());
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		if (primaryCache != null) {
			primaryCache.put(db, pks, entities);
			primaryCache.incrCount(db, pks.length);
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
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		// 清理缓存
		if (primaryCache != null) {
			Number[] pks = new Number[entities.size()];
			for (int i = 0; i < entities.size(); i++) {
				pks[i] = (Number) ReflectUtil.getPkValue(entities.get(i));
			}
			primaryCache.remove(db, pks);
		}

	}

	@Override
	public void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz) {
		removeByPks(new Number[] { pk }, shardingKey, clazz);
	}

	@Override
	public void removeByPks(Number[] pks, IShardingKey<?> shardingKey, Class<?> clazz) {
		String talbeName = ReflectUtil.getTableName(clazz);
		DB db = _getDbFromMaster(talbeName, shardingKey);

		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();
			_removeByPks(conn, pks, clazz, db.getTableIndex());
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
		// 删除缓存
		if (primaryCache != null) {
			primaryCache.remove(db, pks);
			primaryCache.decrCount(db, pks.length);
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

	private void _saveBatch(Connection conn, List<? extends Object> entities, int tableIndex) {
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);

			ps = SQLBuilder.getInsert(conn, entities, tableIndex);
			ps.executeBatch();

			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				LOG.error(e1);
			}
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps, null);
		}
	}

	private void _updateBatchGlobal(Connection conn, List<? extends Object> entities) {
		_updateBatch(conn, entities, -1);
	}

	private void _updateBatch(Connection conn, List<? extends Object> entities, int tableIndex) {
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);

			ps = SQLBuilder.getUpdate(conn, entities, tableIndex);
			ps.executeBatch();
			conn.commit();

		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				LOG.error(e1);
			}
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(null, ps);
		}
	}

	private void _removeByPksGlobal(Connection conn, Number[] pks, Class<?> clazz) {
		_removeByPks(conn, pks, clazz, -1);
	}

	private void _removeByPks(Connection conn, Number[] pks, Class<?> clazz, int tableIndex) {
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
				LOG.error(e1);
			}
		} finally {
			SQLBuilder.close(null, ps);
		}
	}

}
