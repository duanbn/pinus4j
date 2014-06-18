package com.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pinus.api.IShardingKey;
import com.pinus.api.SQL;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.api.query.IQuery;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.datalayer.IShardingSlaveQuery;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBOperationException;
import com.pinus.util.ReflectUtil;

/**
 * 从库查询实现.
 * 
 * @author duanbn
 * 
 */
public class ShardingSlaveQueryImpl extends AbstractShardingQuery implements IShardingSlaveQuery {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(ShardingSlaveQueryImpl.class);

	/**
	 * 数据库集群.
	 */
	private IDBCluster dbCluster;

	@Override
	public <T> T findGlobalOneByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz, EnumDBMasterSlave slave) {
		List<T> entities = findGlobalByQueryFromSlave(query, clusterName, clazz, slave);

		if (entities.isEmpty()) {
			return null;
		}

		return entities.get(0);
	}

	@Override
	public <T> T findOneByQueryFromSlave(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz,
			EnumDBMasterSlave slave) {
		List<T> entities = findByQueryFromSlave(query, shardingValue, clazz, slave);

		if (entities.isEmpty()) {
			return null;
		}

		return entities.get(0);
	}

	@Override
	public Number getGlobalCountFromSlave(String clusterName, Class<?> clazz, EnumDBMasterSlave slave) {
		DBConnectionInfo dbConnInfo = null;
		try {
			dbConnInfo = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		long count = selectCountGlobalWithCache(dbConnInfo, clusterName, clazz).longValue();

		return count;
	}

	@Override
	public Number getGlobalCountFromSlave(String clusterName, SQL<?> sql, EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			DBConnectionInfo dbConnInfo = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);

			conn = dbConnInfo.getDatasource().getConnection();

			return selectCountGlobal(conn, sql);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> T findGlobalByPkFromSlave(Number pk, String clusterName, Class<T> clazz, EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			conn = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave).getDatasource().getConnection();
			return selectByPkWithCache(conn, clusterName, pk, clazz);
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByPksFromSlave(String clusterName, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks) {

		List<T> result = new ArrayList<T>();

		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);

			conn = globalConnection.getDatasource().getConnection();

			result.addAll(selectByPksGlobalWithCache(conn, clusterName, clazz, pks));
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return result;

	}

	@Override
	public <T> List<T> findGlobalByPksFromSlave(List<? extends Number> pks, String clusterName, Class<T> clazz,
			EnumDBMasterSlave slave) {
		return findGlobalByPksFromSlave(clusterName, clazz, slave, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public <T> List<T> findGlobalBySqlFromSlave(SQL<T> sql, String clusterName, EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			DBConnectionInfo slaveGlobal = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);

			conn = slaveGlobal.getDatasource().getConnection();

			List<T> result = null;
			if (isCacheAvailable(sql.getClazz())) {
				Number[] pkValues = selectPksBySqlGlobal(conn, sql);
				result = selectByPksGlobalWithCache(conn, clusterName, sql.getClazz(), pkValues);
			} else {
				result = selectBySqlGlobal(conn, sql);
			}

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public <T> List<T> findGlobalByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz,
			EnumDBMasterSlave slave) {
		Connection conn = null;
		try {
			DBConnectionInfo globalConnection = this.dbCluster.getSlaveGlobalDbConn(clusterName, slave);
			conn = globalConnection.getDatasource().getConnection();

			List<T> result = null;
			if (isCacheAvailable(clazz)) {
				Number[] pkValues = selectPksByQueryGlobal(conn, query, clazz);
				result = selectByPksGlobalWithCache(conn, clusterName, clazz, pkValues);
			} else {
				result = selectByQueryGlobal(conn, query, clazz);
			}

			return result;
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}
	}

	@Override
	public Number getCountFromSlave(IShardingKey<?> shardingValue, Class<?> clazz, EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(slave, clazz, shardingValue);

		return selectCountWithCache(db, clazz);
	}

	@Override
	public Number getCountFromSlave(IShardingKey<?> shardingValue, SQL<?> sql, EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(slave, sql.getClazz(), shardingValue);

		return selectCount(db, sql);
	}

	@Override
	public <T> T findByPkFromSlave(Number pk, IShardingKey<?> shardingValue, Class<T> clazz, EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(slave, clazz, shardingValue);

		return selectByPkWithCache(db, pk, clazz);
	}

	@Override
	public <T> List<T> findByPksFromSlave(IShardingKey<?> shardingValue, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks) {
		DB db = _getDbFromSlave(slave, clazz, shardingValue);

		return selectByPksWithCache(db, clazz, pks);
	}

	@Override
	public <T> List<T> findByPkListFromSlave(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<T> clazz,
			EnumDBMasterSlave slave) {
		return findByPksFromSlave(shardingValue, clazz, slave, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public <T> List<T> findByShardingPairFromSlave(List<IShardingKey<?>> shardingValues, Class<T> clazz,
			EnumDBMasterSlave slave, Number... pks) {
		if (shardingValues.size() != pks.length) {
			throw new DBOperationException("分库分表列表和主键数量不等");
		}

		List<T> result = new ArrayList<T>(pks.length);
		IShardingKey<?> shardingValue = null;
		Number pk = null;
		DB db = null;
		T data = null;
		for (int i = 0; i < pks.length; i++) {
			shardingValue = shardingValues.get(i);
			pk = pks[i];
			db = _getDbFromSlave(slave, clazz, shardingValue);

			data = selectByPkWithCache(db, pk, clazz);
			if (data != null) {
				result.add(data);
			}
		}

		return result;
	}

	@Override
	public <T> List<T> findByShardingPairFromSlave(List<Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz, EnumDBMasterSlave slave) {
		return findByShardingPairFromSlave(shardingValues, clazz, slave, pks.toArray(new Number[pks.size()]));
	}

	@Override
	public <T> List<T> findBySqlFromSlave(SQL<T> sql, IShardingKey<?> shardingValue, EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(slave, sql.getClazz(), shardingValue);

		List<T> result = null;
		if (isCacheAvailable(sql.getClazz())) {
			Number[] pkValues = selectPksBySql(db, sql);
			result = selectByPksWithCache(db, sql.getClazz(), pkValues);
		} else {
			result = selectBySql(db, sql);
		}

		return result;
	}

	@Override
	public <T> List<T> findByQueryFromSlave(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz,
			EnumDBMasterSlave slave) {
		DB db = _getDbFromSlave(slave, clazz, shardingValue);

		List<T> result = null;
		if (isCacheAvailable(clazz)) {
			Number[] pkValues = selectPksByQuery(db, query, clazz);
			result = selectByPksWithCache(db, clazz, pkValues);
		} else {
			result = selectByQuery(db, query, clazz);
		}

		return result;
	}

	@Override
	public void setDBCluster(IDBCluster dbCluster) {
		this.dbCluster = dbCluster;
	}

	/**
	 * 路由选择.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param shardingValue
	 *            路由因子
	 */
	private DB _getDbFromSlave(EnumDBMasterSlave slave, Class<?> clazz, IShardingKey<?> shardingValue) {
		String tableName = ReflectUtil.getTableName(clazz);
		DB db = null;
		try {
			db = this.dbCluster.selectDbFromSlave(slave, tableName, shardingValue);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + db + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return db;
	}

}
