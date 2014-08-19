package com.pinus.api;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.entity.TestEntity;

public class LoadTest extends BaseTest {

	private static final CountDownLatch cdl = new CountDownLatch(99999999);

	public void test() throws Exception {
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

}
