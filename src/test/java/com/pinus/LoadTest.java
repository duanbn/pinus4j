package com.pinus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

import com.pinus.api.IShardingKey;
import com.pinus.api.IShardingStorageClient;
import com.pinus.api.ShardingKey;
import com.pinus.entity.TestEntity;

public class LoadTest extends BaseTest {

	private static final CountDownLatch cdl = new CountDownLatch(99999999);

	private static int count;

	public void loadTest() throws Exception {
		for (int i = 0; i < 200; i++) {
			new Thread() {
				public void run() {
					while (true) {
						TestEntity entity = createEntity();
						Number id = cacheClient.save(entity);

						IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, id);
						cacheClient.removeByPk(id, key, TestEntity.class);

						cdl.countDown();
					}
				}
			}.start();
		}

		cdl.await();
	}

	@Test
	public void testDisLock() throws Exception {
		for (int j = 0; j < 100; j++) {
			Counter c = new Counter(this.cacheClient);
			c.start();
		}

		while (true) {
			Thread.sleep(1000);
			if (count == 200) {
				break;
			}
		}
	}

	public static class Counter extends Thread {
		private IShardingStorageClient client;

		public Counter(IShardingStorageClient client) {
			this.client = client;
		}

		@Override
		public void run() {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				Lock lock = client.createLock("test");
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
