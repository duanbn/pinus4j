package com.pinus.cluster.lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.pinus.config.IClusterConfig;

/**
 * 基于zookeeper实现的集群锁. 集群锁用于处于在多进程环境下的同步问题. 此锁是采用公平算法
 * 
 * <pre>
 * try {
 * 	lock = new DistributedLock(&quot;127.0.0.1:2182&quot;, &quot;test&quot;);
 * 	lock.lock();
 * 	// do something...
 * } catch (Exception e) {
 * 	LOG.error(e)
 * } finally {
 * 	if (lock != null)
 * 		lock.unlock();
 * }
 * </pre>
 * 
 * @author duanbn
 * 
 */
public class DistributedLock implements Lock {

	public static final Logger LOG = Logger.getLogger(DistributedLock.class);

	private static final ReentrantLock threadLock = new ReentrantLock();
	private boolean isThreadLock;

	private ZooKeeper zk;
	private String root = "/locks";// 根
	private String lockName;// 竞争资源的标志
	private String waitNode;// 等待前一个锁
	private String myZnode;// 当前锁
	private CountDownLatch latch;// 计数器
	private int lockTimeout = 30000;
	private List<Exception> exception = new ArrayList<Exception>();

	/**
	 * 创建分布式锁,使用前请确认zkUrl配置的zookeeper服务可用
	 * 
	 * @param zkUrl
	 *            127.0.0.1:2181
	 * @param lockName
	 *            竞争资源标志,lockName中不能包含单词lock
	 */
	public DistributedLock(String lockName, boolean isThreadLock, IClusterConfig config) {
		this.isThreadLock = isThreadLock;

		// 创建一个与服务器的连接
		try {
			this.zk = config.getZooKeeper();

			Stat stat = zk.exists(root, false);
			if (stat == null) {
				// 创建根节点
				zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}

		this.lockName = lockName;
	}

	public void lock() {
		if (exception.size() > 0) {
			throw new LockException(exception.get(0));
		}
		try {
			if (this.tryLock()) {
				if (LOG.isDebugEnabled())
					LOG.debug("Thread " + Thread.currentThread().getId() + " " + myZnode + " get lock true");
				return;
			} else {
				waitForLock(waitNode, lockTimeout);// 等待锁
			}
		} catch (KeeperException e) {
			throw new LockException(e);
		} catch (InterruptedException e) {
			throw new LockException(e);
		}
	}

	public boolean tryLock() {
		if (isThreadLock)
			threadLock.lock();

		try {
			String splitStr = "_lock_";
			if (lockName.contains(splitStr))
				throw new LockException("lockName can not contains \\u000B");
			// 创建临时子节点
			myZnode = zk.create(root + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			// 取出所有子节点
			List<String> subNodes = zk.getChildren(root, false);
			// 取出所有lockName的锁
			List<String> lockObjNodes = new ArrayList<String>();
			for (String node : subNodes) {
				String _node = node.split(splitStr)[0];
				if (_node.equals(lockName)) {
					lockObjNodes.add(node);
				}
			}
			Collections.sort(lockObjNodes);
			if (myZnode.equals(root + "/" + lockObjNodes.get(0))) {
				// 如果是最小的节点,则表示取得锁
				return true;
			}
			// 如果不是最小的节点，找到比自己小1的节点
			String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
			waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZnode) - 1);
		} catch (KeeperException e) {
			throw new LockException(e);
		} catch (InterruptedException e) {
			throw new LockException(e);
		}
		return false;
	}

	public boolean tryLock(long time, TimeUnit unit) {
		try {
			if (this.tryLock()) {
				return true;
			}
			return waitForLock(waitNode, time);
		} catch (Exception e) {
			LOG.error(e);
		}
		return false;
	}

	private boolean waitForLock(String lower, long waitTime) throws InterruptedException, KeeperException {
		Stat stat = zk.exists(root + "/" + lower, true);
		// 判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
		if (stat != null) {
			if (LOG.isDebugEnabled())
				LOG.debug("Thread " + Thread.currentThread().getId() + " waiting for " + root + "/" + lower);
			this.latch = new CountDownLatch(1);
			this.latch.await(waitTime, TimeUnit.MILLISECONDS);
			this.latch = null;
		}
		return true;
	}

	public void unlock() {
		try {
			if (LOG.isDebugEnabled())
				LOG.debug("unlock " + myZnode);
			zk.delete(myZnode, -1);
			myZnode = null;
			zk.close();
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (KeeperException e) {
			LOG.error(e);
		} finally {
			if (isThreadLock)
				threadLock.unlock();
		}
	}

	public void lockInterruptibly() throws InterruptedException {
		this.lock();
	}

	public Condition newCondition() {
		return null;
	}

	public class LockException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public LockException(String e) {
			super(e);
		}

		public LockException(Exception e) {
			super(e);
		}
	}

}
