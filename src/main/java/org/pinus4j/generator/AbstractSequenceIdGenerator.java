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

package org.pinus4j.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.constant.Const;
import org.pinus4j.exceptions.DBOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public static final Logger LOG = LoggerFactory.getLogger(AbstractDBGenerator.class);

	/**
	 * 批量生成id缓冲
	 */
	private final Map<String, Queue<Long>> longIdBuffer = new HashMap<String, Queue<Long>>();
	private int BUFFER_SIZE;
	private ZooKeeper zk;

	public AbstractSequenceIdGenerator(CuratorFramework curatorClient, int bufferSize) {
		BUFFER_SIZE = bufferSize;

		// 创建一个与服务器的连接
		try {
			this.zk = curatorClient.getZookeeperClient().getZooKeeper();
			Stat stat = zk.exists(Const.ZK_PRIMARYKEY, false);
			if (stat == null) {
				// 创建根节点
				zk.create(Const.ZK_PRIMARYKEY, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String getBufferKey(String clusterName, String name) {
		return clusterName + name;
	}

	@Override
	public void checkAndSetPrimaryKey(long pk, String clusterName, String name) {
		Lock lock = getLock(clusterName + name);

		try {
			lock.lock();
			String pkNode = Const.ZK_PRIMARYKEY + "/" + clusterName + "/" + name;
			Stat stat = zk.exists(pkNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(pkNode, String.valueOf(pk).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} else {
				byte[] data = zk.getData(pkNode, false, null);
				long currentPk = Long.parseLong(new String(data));
				if (pk > currentPk) {
					zk.setData(pkNode, String.valueOf(pk).getBytes(), -1);
				}
			}
		} catch (Exception e) {
			throw new DBOperationException("校验主键值失败");
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int genClusterUniqueIntId(String clusterName, String name) {
		return genClusterUniqueIntId(clusterName, name, 0);
	}

	@Override
	public int genClusterUniqueIntId(String clusterName, String name, long seed) {
		long id = _genId(clusterName, name, seed);

		if (id == 0) {
			int retry = 5;
			while (retry-- == 0) {
				id = _genId(clusterName, name, seed);
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
	public long genClusterUniqueLongId(String clusterName, String name) {
		return genClusterUniqueLongId(clusterName, name, 0);
	}

	@Override
	public long genClusterUniqueLongId(String clusterName, String name, long seed) {
		long id = _genId(clusterName, name, seed);

		if (id == 0) {
			int retry = 5;
			while (retry-- == 0) {
				id = _genId(clusterName, name, seed);
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

	private long _genId(String clusterName, String name, long seed) {
		Queue<Long> buffer = null;
		synchronized (longIdBuffer) {
			buffer = longIdBuffer.get(getBufferKey(clusterName, name));
			if (buffer != null && !buffer.isEmpty()) {
				long id = buffer.poll();
				return id;
			} else if (buffer == null || buffer.isEmpty()) {
				buffer = new ConcurrentLinkedQueue<Long>();
				long[] newIds = _genClusterUniqueLongIdBatch(clusterName, name, BUFFER_SIZE, seed);
				for (long newId : newIds) {
					buffer.offer(newId);
				}
				longIdBuffer.put(getBufferKey(clusterName, name), buffer);
			}
		}

		Long id = buffer.poll();

		if (id == 0) {
			throw new RuntimeException("生成id失败");
		}

		return id;
	}

	@Override
	public int[] genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize) {
		int[] intIds = _genClusterUniqueIntIdBatch(clusterName, name, batchSize, 0);
		return intIds;
	}

	@Override
	public int[] genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize, long seed) {
		int[] intIds = _genClusterUniqueIntIdBatch(clusterName, name, batchSize, seed);
		return intIds;
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize) {
		long[] longIds = _genClusterUniqueLongIdBatch(clusterName, name, batchSize, 0);
		return longIds;
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize, long seed) {
		long[] longIds = _genClusterUniqueLongIdBatch(clusterName, name, batchSize, seed);
		return longIds;
	}

	/**
	 * 生成n个int型的数值
	 * 
	 * @param clusterName
	 * @param name
	 * @param batchSize
	 * @param seed
	 *            当seed大于当前值则使用seed作为起点
	 * @return
	 */
	private int[] _genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize, long seed) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("参数错误, batchSize不能小于0");
		}

		Lock lock = getLock(name);

		int[] ids = new int[batchSize];
		try {
			lock.lock();

			String clusterNode = clusterName;
			Stat stat = zk.exists(clusterNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(clusterNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			long pk = 0;
			long nodeValue = seed;
			String pkNode = clusterNode + "/" + name;
			stat = zk.exists(pkNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(pkNode, String.valueOf(nodeValue + batchSize).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else {
				pk = Long.parseLong(new String(zk.getData(pkNode, false, null)));
				if (pk > nodeValue) {
					nodeValue = pk;
				}
			}

			for (int i = 1; i <= batchSize; i++) {
				ids[i - 1] = (int) (nodeValue + i);
			}

			zk.setData(pkNode, String.valueOf(nodeValue += batchSize).getBytes(), -1);
		} catch (Exception e) {
			throw new DBOperationException("生成唯一id失败", e);
		} finally {
			lock.unlock();
		}

		return ids;
	}

	/**
	 * 生成n个long型的数值
	 * 
	 * @param clusterName
	 * @param name
	 * @param batchSize
	 * @param seed
	 *            当seed大于当前值则使用seed作为起点
	 * @return
	 */
	private long[] _genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize, long seed) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("参数错误, batchSize不能小于0");
		}

		Lock lock = getLock(name);

		long[] ids = new long[batchSize];
		try {
			lock.lock();

			String clusterNode = clusterName;
			Stat stat = zk.exists(clusterNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(clusterNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			long pk = 0;
			long nodeValue = seed;
			String pkNode = clusterNode + "/" + name;
			stat = zk.exists(pkNode, false);
			if (stat == null) {
				// 创建根节点
				zk.create(pkNode, String.valueOf(nodeValue + batchSize).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else {
				pk = Long.parseLong(new String(zk.getData(pkNode, false, null)));
				if (pk > nodeValue) {
					nodeValue = pk;
				}
			}

			for (int i = 1; i <= batchSize; i++) {
				ids[i - 1] = nodeValue + i;
			}

			zk.setData(pkNode, String.valueOf(nodeValue += batchSize).getBytes(), -1);
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
