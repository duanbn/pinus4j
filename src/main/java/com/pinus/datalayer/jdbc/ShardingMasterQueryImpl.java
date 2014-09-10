package com.pinus.datalayer.jdbc;

import java.sql.Connection;
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

		try {
			T obj = (T) ReflectUtil.cloneWithGivenField(entities.get(0));

			return obj;
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public <T> T findOneByQueryFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
		List<T> entities = findByQueryFromMaster(query, shardingKey, clazz);

		if (entities.isEmpty()) {
			return null;
		}

		if (entities.size() > 1) {
			throw new DBOperationException("查询结果大于1条记录");
		}

		try {
			T obj = (T) ReflectUtil.cloneWithGivenField(entities.get(0));

			return obj;
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public Number getGlobalCountFromMaster(String clusterName, Class<?> clazz) {
		DBConnectionInfo globalConnection;
		try {
			globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectGlobalCountWithCache(globalConnection, clusterName, clazz).longValue();

		return count;
	}

    @Override
    public Number getGlobalCountFromMaster(IQuery query, String clusterName, Class<?> clazz) {
        DBConnectionInfo globalConnection;
		try {
			globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectGlobalCount(query, globalConnection, clusterName, clazz).longValue();

		return count;
    }

	@Override
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();
			return selectByPkWithCache(conn, clusterName, pk, clazz);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, Number... pks) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();
			return selectGlobalByPksWithCache(conn, clusterName, clazz, pks);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromMaster(List<? extends Number> pks, String clusterName, Class<T> clazz) {
		return findGlobalByPksFromMaster(clusterName, clazz, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public List<Map<String, Object>> findGlobalBySqlFromMaster(SQL sql, String clusterName) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			List<Map<String, Object>> result = selectGlobalBySql(conn, sql);

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getMasterGlobalConn(clusterName);

			conn = globalConnection.getDatasource().getConnection();

			List<T> result = null;
			if (isCacheAvailable(clazz)) {
				Number[] pkValues = selectGlobalPksByQuery(conn, query, clazz);
				result = selectGlobalByPksWithCache(conn, clusterName, clazz, pkValues);

				// 过滤从缓存结果, 将没有指定的字段设置为默认值.
				List<T> filteResult = new ArrayList<T>(result.size());
				if (query.hasQueryFields()) {
					for (T obj : result) {
						filteResult.add((T) ReflectUtil.cloneWithGivenField(obj));
					}
					result = filteResult;
				}
			} else {
				result = selectGlobalByQuery(conn, query, clazz);
			}

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public Number getCountFromMaster(Class<?> clazz) {
		List<DB> dbs = this.dbCluster.getAllMasterShardingDB(clazz);
		long count = 0;
		for (DB db : dbs) {
			count += selectCountWithCache(db, clazz).longValue();
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
	public Number getCountFromMaster(IShardingKey<?> shardingKey, Class<?> clazz) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectCountWithCache(db, clazz);
	}

	@Override
	public Number getCountFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectCount(db, clazz, query);
	}

	@Override
	public <T> T findByPkFromMaster(Number pk, IShardingKey<?> shardingKey, Class<T> clazz) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectByPkWithCache(db, pk, clazz);
	}

	@Override
	public <T> List<T> findByPksFromMaster(IShardingKey<?> shardingKey, Class<T> clazz, Number... pks) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		return selectByPksWithCache(db, clazz, pks);
	}

	@Override
	public <T> List<T> findByPkListFromMaster(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz) {
		return findByPksFromMaster(shardingKey, clazz, pks.toArray(new Number[pks.size()]));
	}

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

			data = selectByPkWithCache(db, pk, clazz);
			if (data != null) {
				result.add(data);
			}
		}

		return result;
	}

	@Override
	public <T> List<T> findByShardingPairFromMaster(List<? extends Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz) {
		return findByShardingPairFromMaster(shardingValues, clazz, pks.toArray(new Number[pks.size()]));
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
	public <T> List<T> findByQueryFromMaster(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
		DB db = _getDbFromMaster(clazz, shardingKey);

		List<T> result = null;
		if (isCacheAvailable(clazz)) {
			Number[] pkValues = selectPksByQuery(db, query, clazz);
			result = selectByPksWithCache(db, clazz, pkValues);

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
		} else {
			result = selectByQuery(db, query, clazz);
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
