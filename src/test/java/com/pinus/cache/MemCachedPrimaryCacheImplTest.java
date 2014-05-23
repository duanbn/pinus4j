package com.pinus.cache;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.IShardingKey;
import com.pinus.api.ShardingKey;
import com.pinus.cluster.DB;

public class MemCachedPrimaryCacheImplTest extends BaseTest {

	@Test
	public void decrCount() throws Exception {
		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_NAME, 1);
		DB db = client.getDbCluster().selectDbFromMaster("test_entity", shardingValue);
		long count = primaryCache.decrCount(db, 1);
		System.out.println(count);
	}

	@Test
	public void incrCount() throws Exception {
		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_NAME, 1);
		DB db = client.getDbCluster().selectDbFromMaster("test_entity", shardingValue);
		long count = primaryCache.incrCount(db, 1);
		System.out.println(count);
	}

	@Test
	public void getCount() throws Exception {
		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_NAME, 1);
		DB db = client.getDbCluster().selectDbFromMaster("test_entity", shardingValue);
		long count = primaryCache.getCount(db);
		System.out.println(count);
	}

	@Test
	public void put() throws Exception {
		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_NAME, 1);
		DB db = client.getDbCluster().selectDbFromMaster("test_entity", shardingValue);
		primaryCache.put(db, 1, "test cache");
		Assert.assertEquals("test cache", primaryCache.get(db, 1));

		for (int i = 1; i <= 10; i++) {
			primaryCache.put(db, i, "test cache batch " + i);
		}
		List<String> data = primaryCache.get(db, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		for (int i = 0; i < 10; i++) {
			Assert.assertEquals("test cache batch " + (i + 1), data.get(i));
		}
	}

	@Test
	public void remove() throws Exception {
		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_NAME, 1);
		DB db = client.getDbCluster().selectDbFromMaster("test_entity", shardingValue);
		primaryCache.remove(db, 1);
		Assert.assertNull(primaryCache.get(db, 1));

		for (int i = 2; i <= 10; i++) {
			primaryCache.remove(db, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		}
		for (int i = 2; i <= 10; i++) {
			Assert.assertNull(primaryCache.get(db, i));
		}
	}

}
