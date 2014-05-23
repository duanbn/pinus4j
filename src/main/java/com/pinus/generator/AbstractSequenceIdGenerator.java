package com.pinus.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import com.pinus.cluster.IDBCluster;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlDBClusterConfigImpl;
import com.pinus.exception.DBOperationException;
import com.pinus.exception.LoadConfigException;
import com.pinus.util.StringUtils;

/**
 * 抽象的ID生成器.
 * 
 * @author duanbn
 * 
 */
public abstract class AbstractSequenceIdGenerator implements IIdGenerator, Watcher {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(AbstractDBGenerator.class);

	/**
	 * 批量生成id缓冲
	 */
	private final Map<String, Queue<Long>> longIdBuffer = new HashMap<String, Queue<Long>>();
	private int BUFFER_SIZE;

	private final String zkUrl;
	private ZooKeeper zk;
	private CountDownLatch connectedLatch = new CountDownLatch(1);
	private int sessionTimeout = 30000;
	private String root = "/primarykey";// 根

	/**
	 * zookeeper节点的监视器
	 */
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedLatch.countDown();
		}
	}

	public AbstractSequenceIdGenerator() {
		IClusterConfig config;
		try {
			config = XmlDBClusterConfigImpl.getInstance();
		} catch (LoadConfigException e) {
			throw new RuntimeException(e);
		}
		BUFFER_SIZE = config.getIdGeneratorBatch();

		zkUrl = config.getZkUrl();
		if (StringUtils.isBlank(zkUrl)) {
			throw new RuntimeException("读取zk url失败");
		}

		// 创建一个与服务器的连接
		try {
			zk = new ZooKeeper(this.zkUrl, sessionTimeout, this);
			if (States.CONNECTING == zk.getState()) {
				connectedLatch.await();
			}
			Stat stat = zk.exists(root, false);
			if (stat == null) {
				// 创建根节点
				zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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

		if (id == 0) {
			throw new RuntimeException("生成id失败");
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
		
		if (id == 0) {
			throw new RuntimeException("生成id失败");
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
		
		if (id == 0) {
			throw new RuntimeException("生成id失败");
		}
		
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

		Lock lock = getLock(name);

		long[] ids = new long[batchSize];
		try {
			lock.lock();

			String clusterNode = root + "/" + clusterName;
			Stat stat = zk.exists(clusterNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(clusterNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			long pk = 0;
			String pkNode = clusterNode + "/" + name;
			stat = zk.exists(pkNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(pkNode, String.valueOf(batchSize).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else {
				byte[] data = zk.getData(pkNode, false, null);
				pk = Long.parseLong(new String(data));
			}

			for (int i = 1; i <= batchSize; i++) {
				ids[i - 1] = pk + i;
			}

			zk.setData(pkNode, String.valueOf(pk += batchSize).getBytes(), -1);
		} catch (Exception e) {
			throw new DBOperationException("生成唯一id失败", e);
		} finally {
			lock.unlock();
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
