package com.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pinus.api.IShardingKey;
import com.pinus.api.SQL;
import com.pinus.api.query.IQuery;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.datalayer.IShardingMasterQuery;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBOperationException;
import com.pinus.util.ReflectUtil;

/**
 * 分库分表查询实现.
 * 
 * @author duanbn
 */
public class ShardingMasterQueryImpl extends AbstractShardingQuery implements IShardingMasterQuery {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(ShardingMasterQueryImpl.class);

	/**
	 * 数据库集群引用.
	 */
	private IDBCluster dbCluster;

	@Override
	public <T> T findGlobalOneByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz) {
		List<T> entities = findGlobalByQueryFromMaster(query, clusterName, clazz);

		if (entities.isEmpty()) {
			return null;
		}
		return entities.get(0);
	}

	@Override
	public <T> T findOneByQueryFromMaster(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz) {
		List<T> entities = findByQueryFromMaster(query, shardingValue, clazz);

		if (entities.isEmpty()) {
			return null;
		}

		if (entities.size() > 1) {
			throw new DBOperationException("查询结果大于1条记录");
		}

		return entities.get(0);
	}

	@Override
	public Number getGlobalCountFromMaster(String clusterName, Class<?> clazz) {
		List<DBConnectionInfo> dbConnInfos = this.dbCluster.getMasterGlobalDbConn(clusterName);
		long count = selectCountGlobalWithCache(dbConnInfos, clusterName, clazz).longValue();

		return count;
	}

	@Override
	public Number getGlobalCountFromMaster(String clusterName, SQL<?> sql) {
		long count = 0;
		List<DBConnectionInfo> dbConnInfos = this.dbCluster.getMasterGlobalDbConn(clusterName);
		for (DBConnectionInfo dbConnInfo : dbConnInfos) {
			Connection conn = null;
			try {
				conn = dbConnInfo.getDatasource().getConnection();
				count += selectCountGlobal(conn, sql).longValue();
			} catch (SQLException e) {
				throw new DBOperationException(e);
			} finally {
				SQLBuilder.close(conn);
			}
		}

		return count;

	}

	@Override
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz) {
		Connection conn = null;
		try {
			conn = this.dbCluster.getMasterGlobalDbConn(pk, clusterName).getDatasource().getConnection();
			return selectByPkWithCache(conn, clusterName, pk, clazz);
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, Number... pks) {
		Map<DBConnectionInfo, List<Number>> map = this.dbCluster.getMasterGlobalDbConn(pks, clusterName);

		List<T> result = new ArrayList<T>();

		for (Map.Entry<DBConnectionInfo, List<Number>> entry : map.entrySet()) {
			Connection conn = null;
			try {
				conn = entry.getKey().getDatasource().getConnection();
				result.addAll(selectByPksGlobalWithCache(conn, clusterName, clazz,
						entry.getValue().toArray(new Number[entry.getValue().size()])));
			} catch (SQLException e) {
				throw new DBOperationException(e);
			} finally {
				SQLBuilder.close(conn);
			}
		}

		return result;
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(List<? extends Number> pks, String clusterName, Class<T> clazz) {
		return findGlobalByPksFromMaster(clusterName, clazz, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public <T> List<T> findGlobalBySqlFromMaster(SQL<T> sql, String clusterName) {
		throw new UnsupportedOperationException();
		// Connection conn = null;
		// try {
		// conn = this.dbCluster.getMasterGlobalDbConn(clusterName);
		// List<T> result = null;
		// if (isCacheAvailable(sql.getClazz())) {
		// Number[] pkValues = selectPksBySqlGlobal(conn, sql);
		// result = selectByPksGlobalWithCache(conn, clusterName,
		// sql.getClazz(), pkValues);
		// } else {
		// result = selectBySqlGlobal(conn, sql);
		// }
		//
		// return result;
		// } catch (SQLException e) {
		// throw new DBOperationException(e);
		// } finally {
		// SQLBuilder.close(conn);
		// }
	}

	@Override
	public <T> List<T> findGlobalByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz) {
		throw new UnsupportedOperationException();
		// Connection conn = null;
		// try {
		// conn = this.dbCluster.getMasterGlobalDbConn(clusterName);
		//
		// List<T> result = null;
		// if (isCacheAvailable(clazz)) {
		// Number[] pkValues = selectPksByQueryGlobal(conn, query, clazz);
		// result = selectByPksGlobalWithCache(conn, clusterName, clazz,
		// pkValues);
		// } else {
		// result = selectByQueryGlobal(conn, query, clazz);
		// }
		//
		// return result;
		// } catch (SQLException e) {
		// throw new DBOperationException(e);
		// } finally {
		// SQLBuilder.close(conn);
		// }
	}

	@Override
	public Number getCountFromMaster(IShardingKey<?> shardingValue, Class<?> clazz) {
		DB db = _getDbFromMaster(clazz, shardingValue);

		try {
			return selectCountWithCache(db, clazz);
		} finally {
			SQLBuilder.close(db.getDbConn());
		}
	}

	@Override
	public Number getCountFromMaster(IShardingKey<?> shardingValue, SQL<?> sql) {
		DB db = _getDbFromMaster(sql.getClazz(), shardingValue);

		try {
			return selectCount(db, sql);
		} finally {
			SQLBuilder.close(db.getDbConn());
		}
	}

	@Override
	public <T> T findByPkFromMaster(Number pk, IShardingKey<?> shardingValue, Class<T> clazz) {
		DB db = _getDbFromMaster(clazz, shardingValue);

		try {
			return selectByPkWithCache(db, pk, clazz);
		} finally {
			SQLBuilder.close(db.getDbConn());
		}
	}

	@Override
	public <T> List<T> findByPksFromMaster(IShardingKey<?> shardingValue, Class<T> clazz, Number... pks) {
		DB db = _getDbFromMaster(clazz, shardingValue);

		try {
			return selectByPksWithCache(db, clazz, pks);
		} finally {
			SQLBuilder.close(db.getDbConn());
		}
	}

	@Override
	public <T> List<T> findByPkListFromMaster(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<T> clazz) {
		return findByPksFromMaster(shardingValue, clazz, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public <T> List<T> findByShardingPairFromMaster(List<IShardingKey<?>> shardingValues, Class<T> clazz, Number... pks) {
		if (shardingValues.size() != pks.length) {
			throw new DBOperationException("分库分表列表和主键数量不等");
		}

		List<T> result = new ArrayList<T>(pks.length);
		IShardingKey<?> shardingValue = null;
		Number pk = null;
		DB db = null;
		T data = null;
		try {
			for (int i = 0; i < pks.length; i++) {
				shardingValue = shardingValues.get(i);
				pk = pks[i];
				db = _getDbFromMaster(clazz, shardingValue);

				data = selectByPkWithCache(db, pk, clazz);
				if (data != null) {
					result.add(data);
				}
			}
		} finally {
			SQLBuilder.close(db.getDbConn());
		}

		return result;
	}

	@Override
	public <T> List<T> findByShardingPairFromMaster(List<? extends Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz) {
		return findByShardingPairFromMaster(shardingValues, clazz, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public <T> List<T> findBySqlFromMaster(SQL<T> sql, IShardingKey<?> shardingValue) {
		DB db = _getDbFromMaster(sql.getClazz(), shardingValue);

		List<T> result = null;
		try {
			if (isCacheAvailable(sql.getClazz())) {
				Number[] pkValues = selectPksBySql(db, sql);
				result = selectByPksWithCache(db, sql.getClazz(), pkValues);
			} else {
				result = selectBySql(db, sql);
			}
		} finally {
			SQLBuilder.close(db.getDbConn());
		}

		return result;
	}

	@Override
	public <T> List<T> findByQueryFromMaster(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz) {
		DB db = _getDbFromMaster(clazz, shardingValue);

		List<T> result = null;
		try {
			if (isCacheAvailable(clazz)) {
				Number[] pkValues = selectPksByQuery(db, query, clazz);
				result = selectByPksWithCache(db, clazz, pkValues);
			} else {
				result = selectByQuery(db, query, clazz);
			}
		} finally {
			SQLBuilder.close(db.getDbConn());
		}

		return result;
	}

	/**
	 * 设置数据库集群.
	 */
	public void setDBCluster(IDBCluster dbCluster) {
		this.dbCluster = dbCluster;
	}

	/**
	 * 获取数据库集群
	 */
	public IDBCluster getDBCluster() {
		return this.dbCluster;
	}

	/**
	 * 路由选择.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param shardingValue
	 *            路由因子
	 */
	private DB _getDbFromMaster(Class<?> clazz, IShardingKey<?> shardingValue) {
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

}
