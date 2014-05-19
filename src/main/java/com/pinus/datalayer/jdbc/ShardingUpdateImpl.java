package com.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pinus.api.IShardingValue;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
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
		Connection conn = null;
		try {
			for (int i = 0; i < entities.size(); i++) {
				ReflectUtil.setPkValue(entities.get(i), newPks[i]);
				pks[i] = newPks[i];
			}

			conn = this.dbCluster.getMasterGlobalDbConn(clusterName);

			_saveBatchGlobal(conn, entities);
		} catch (Exception e1) {
			throw new DBOperationException(e1);
		} finally {
			SQLBuilder.close(conn);
		}

		if (primaryCache != null) {
			primaryCache.putGlobal(clusterName, tableName, pks, entities);
			primaryCache.incrCountGlobal(clusterName, tableName, pks.length);
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
			conn = this.dbCluster.getMasterGlobalDbConn(clusterName);

			_updateBatchGlobal(conn, entities);

			Number[] pks = new Number[entities.size()];
			for (int i = 0; i < entities.size(); i++) {
				try {
					pks[i] = ReflectUtil.getPkValue(entities.get(i));
				} catch (Exception e) {
					throw new DBOperationException("获取更新数据的主键值失败");
				}
			}

			// 更新缓存
			if (primaryCache != null) {
				primaryCache.putGlobal(clusterName, tableName, pks, entities);
			}
		} catch (SQLException e1) {
			throw new DBOperationException(e1);
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
			conn = this.dbCluster.getMasterGlobalDbConn(clusterName);

			_removeByPksGlobal(conn, pks, clazz);

			// 删除缓存
			if (primaryCache != null) {
				String tableName = ReflectUtil.getTableName(clazz);
				primaryCache.removeGlobal(clusterName, tableName, pks);
				primaryCache.decrCountGlobal(clusterName, tableName, pks.length);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public Number save(Object entity, IShardingValue<?> shardingValue) {
		List<Object> entities = new ArrayList<Object>(1);
		entities.add(entity);
		return saveBatch(entities, shardingValue)[0];
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingValue<?> shardingValue) {
		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		DB db = _getDbFromMaster(clazz, shardingValue);

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

			conn = db.getDbConn();

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
	public void update(Object entity, IShardingValue<?> shardingValue) {
		List<Object> entities = new ArrayList<Object>();
		entities.add(entity);
		updateBatch(entities, shardingValue);
	}

	@Override
	public void updateBatch(List<? extends Object> entities, IShardingValue<?> shardingValue) {
		Class<?> entityClass = entities.get(0).getClass();

		DB db = _getDbFromMaster(entityClass, shardingValue);
		Connection conn = null;
		try {
			conn = db.getDbConn();
			_updateBatch(conn, entities, db.getTableIndex());
		} finally {
			SQLBuilder.close(conn);
		}

		// 更新缓存
		if (primaryCache != null) {
			Number[] pks = new Number[entities.size()];
			for (int i = 0; i < entities.size(); i++) {
				try {
					pks[i] = (Number) ReflectUtil.getPkValue(entities.get(i));
				} catch (Exception e) {
					throw new DBOperationException("获取更新数据的主键值失败");
				}
			}
			primaryCache.put(db, pks, entities);
		}

	}

	@Override
	public void removeByPk(Number pk, IShardingValue<?> shardingValue, Class<?> clazz) {
		removeByPks(new Number[] { pk }, shardingValue, clazz);
	}

	@Override
	public void removeByPks(Number[] pks, IShardingValue<?> shardingValue, Class<?> clazz) {
		DB db = _getDbFromMaster(clazz, shardingValue);

		Connection conn = null;
		try {
			conn = db.getDbConn();
			_removeByPks(conn, pks, clazz, db.getTableIndex());
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
	 * @param shardingValue
	 *            路由因子
	 */
	private DB _getDbFromMaster(Class<?> clazz, IShardingValue<?> shardingValue) {
		String tableName = ReflectUtil.getTableName(clazz);
		DB db = null;
		try {
			db = this.dbCluster.selectDbFromMaster(tableName, shardingValue);
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
