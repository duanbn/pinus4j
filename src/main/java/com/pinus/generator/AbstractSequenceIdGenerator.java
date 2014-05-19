package com.pinus.generator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlClusterConfigImpl;
import com.pinus.constant.Const;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBOperationException;
import com.pinus.exception.LoadConfigException;

/**
 * 抽象的ID生成器.
 * 
 * @author duanbn
 * 
 */
public abstract class AbstractSequenceIdGenerator implements IIdGenerator {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(AbstractDBGenerator.class);

	/**
	 * 批量生成id缓冲
	 */
	private static final Map<String, Queue<Long>> longIdBuffer = new HashMap<String, Queue<Long>>();
	private static int BUFFER_SIZE;

	static {
		IClusterConfig config;
		try {
			config = XmlClusterConfigImpl.getInstance();
		} catch (LoadConfigException e) {
			throw new RuntimeException(e);
		}
		BUFFER_SIZE = config.getIdGeneratorBatch();
	}

	private String getBufferKey(String clusterName, String name) {
		return clusterName + name;
	}

	@Override
	public synchronized int genClusterUniqueIntId(IDBCluster dbCluster, String clusterName, String name) {
		long id = _genId(dbCluster, clusterName, name);

		if (id == 0) {
			int retry = 5;
			while (retry-- == 0) {
				id = _genId(dbCluster, clusterName, name);
				if (id > 0) {
					break;
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					LOG.warn("生成id=0, 重新生成");
				}
			}
		}

		return new Long(id).intValue();
	}

	@Override
	public synchronized long genClusterUniqueLongId(IDBCluster dbCluster, String clusterName, String name) {
		long id = _genId(dbCluster, clusterName, name);

		if (id == 0) {
			int retry = 5;
			while (retry-- == 0) {
				id = _genId(dbCluster, clusterName, name);
				if (id > 0) {
					break;
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					LOG.warn("生成id=0, 重新生成");
				}
			}
		}

		return id;
	}

	private long _genId(IDBCluster dbCluster, String clusterName, String name) {
		Queue<Long> buffer = longIdBuffer.get(getBufferKey(clusterName, name));
		if (buffer != null && !buffer.isEmpty()) {
			long id = buffer.poll();
			return id;
		} else if (buffer == null || buffer.isEmpty()) {
			buffer = new ConcurrentLinkedQueue<Long>();
			long[] newIds = genClusterUniqueLongIdBatch(dbCluster, clusterName, name, BUFFER_SIZE);
			for (long newId : newIds) {
				buffer.offer(newId);
			}
			longIdBuffer.put(getBufferKey(clusterName, name), buffer);
		}
		Long id = buffer.poll();
		return id;
	}

	@Override
	public int[] genClusterUniqueIntIdBatch(IDBCluster dbCluster, String clusterName, String name, int batchSize) {
		long[] longIds = genClusterUniqueLongIdBatch(dbCluster, clusterName, name, batchSize);
		int[] intIds = new int[longIds.length];
		for (int i = 0; i < longIds.length; i++) {
			intIds[i] = new Long(longIds[i]).intValue();
		}
		return intIds;
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(IDBCluster dbCluster, String clusterName, String name, int batchSize) {
		long[] longIds = _genClusterUniqueLongIdBatch(dbCluster, clusterName, name, batchSize);
		return longIds;
	}

	public long[] _genClusterUniqueLongIdBatch(IDBCluster dbCluster, String clusterName, String name, int batchSize) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("参数错误, batchSize不能小于0");
		}

		DB global = null;
		try {
			global = dbCluster.getGlobalIdFromMaster(clusterName);
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}

		String tableName = name;
		Lock lock = getLock(tableName);

		long[] ids = new long[batchSize];
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = global.getDbConn();

			lock.lock();
			// 生成新的id.
			ps = conn.prepareStatement("select " + Const.TABLE_GLOBALID_FIELD_ID + " from " + Const.TABLE_GLOBALID_NAME
					+ " where " + Const.TABLE_GLOBALID_FIELD_TABLENAME + " = ?");
			ps.setString(1, tableName);
			rs = ps.executeQuery();
			long id = 0;
			if (rs.next()) {
				id = rs.getLong(Const.TABLE_GLOBALID_FIELD_ID);
			}
			for (int i = 1; i <= batchSize; i++) {
				ids[i - 1] = id + i;
			}
			id += batchSize;
			rs.close();
			ps.close();

			// 更新global_id表
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("replace into " + Const.TABLE_GLOBALID_NAME + " ("
					+ Const.TABLE_GLOBALID_FIELD_TABLENAME + ", " + Const.TABLE_GLOBALID_FIELD_ID + ") values (?, ?)");
			ps.setString(1, tableName);
			ps.setLong(2, id);
			ps.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				LOG.error(e1);
			}
			throw new DBOperationException("生成唯一id失败", e);
		} finally {
			lock.unlock();
			SQLBuilder.close(conn, ps, rs);
		}

		return ids;
	}

	/**
	 * 获取集群锁
	 * 
	 * @return
	 */
	public abstract Lock getLock(String lockName);

}
