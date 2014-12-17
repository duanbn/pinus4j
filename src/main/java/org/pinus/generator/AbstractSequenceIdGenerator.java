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

package org.pinus.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.pinus.config.IClusterConfig;
import org.pinus.constant.Const;
import org.pinus.exception.DBOperationException;
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

	public AbstractSequenceIdGenerator(IClusterConfig config) {
		BUFFER_SIZE = config.getIdGeneratorBatch();

		// 创建一个与服务器的连接
		try {
			this.zk = config.getZooKeeper();
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
	public void close() {
		try {
			this.zk.close();
		} catch (Exception e) {
			LOG.warn("close zookeeper client failure");
		}
	}

	@Override
	public synchronized int genClusterUniqueIntId(String clusterName, String name) {
		long id = _genId(clusterName, name);

		if (id == 0) {
			int retry = 5;
			while (retry-- == 0) {
				id = _genId(clusterName, name);
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
	public synchronized long genClusterUniqueLongId(String clusterName, String name) {
		long id = _genId(clusterName, name);

		if (id == 0) {
			int retry = 5;
			while (retry-- == 0) {
				id = _genId(clusterName, name);
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

	private long _genId(String clusterName, String name) {
		Queue<Long> buffer = longIdBuffer.get(getBufferKey(clusterName, name));
		if (buffer != null && !buffer.isEmpty()) {
			long id = buffer.poll();
			return id;
		} else if (buffer == null || buffer.isEmpty()) {
			buffer = new ConcurrentLinkedQueue<Long>();
			long[] newIds = genClusterUniqueLongIdBatch(clusterName, name, BUFFER_SIZE);
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
	public int[] genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize) {
		long[] longIds = genClusterUniqueLongIdBatch(clusterName, name, batchSize);
		int[] intIds = new int[longIds.length];
		for (int i = 0; i < longIds.length; i++) {
			intIds[i] = new Long(longIds[i]).intValue();
		}
		return intIds;
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize) {
		long[] longIds = _genClusterUniqueLongIdBatch(clusterName, name, batchSize);
		return longIds;
	}

	private long[] _genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("参数错误, batchSize不能小于0");
		}

		Lock lock = getLock(name);

		long[] ids = new long[batchSize];
		try {
			lock.lock();

			String clusterNode = Const.ZK_PRIMARYKEY + "/" + clusterName;
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
