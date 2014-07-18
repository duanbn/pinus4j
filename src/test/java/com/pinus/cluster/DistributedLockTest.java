package com.pinus.cluster;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.IShardingStorageClient;
import com.pinus.cluster.lock.DistributedLock;

public class DistributedLockTest extends BaseTest {

	private static int count;

	@Test
	public void testDisLock() throws Exception {
		for (int j = 0; j < 18; j++) {
			Counter c = new Counter(this.cacheClient);
			c.start();
		}

		Thread.sleep(2000);
	}

	public static class Counter extends Thread {
		private IShardingStorageClient client;

		public Counter(IShardingStorageClient client) {
			this.client = client;
		}

		@Override
		public void run() {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				DistributedLock lock = new DistributedLock("test", false, this.client.getDbCluster().getClusterConfig());
				try {
					lock.lock();
					System.out.println(count++);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					lock.unlock();
				}
			}
		}
	}

}
