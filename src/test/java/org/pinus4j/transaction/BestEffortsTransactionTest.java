package org.pinus4j.transaction;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.ApiBaseTest;
import org.pinus4j.api.IShardingKey;
import org.pinus4j.api.ShardingKey;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class BestEffortsTransactionTest extends ApiBaseTest {

	@Test
	public void testCommit() {
		TestGlobalEntity testGlobalEntity = createGlobalEntity();
		TestEntity testEntity = createEntity();

		cacheClient.beginTransaction();
		try {
			long globalId = cacheClient.globalSave(testGlobalEntity).longValue();
			long shardingId = cacheClient.save(testEntity).longValue();

			cacheClient.commit();

			TestGlobalEntity a = cacheClient.findGlobalByPk(globalId, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
			IShardingKey<Integer> sk = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, testEntity.getTestInt());
			TestEntity b = cacheClient.findByPk(shardingId, sk, TestEntity.class);
			Assert.assertEquals(testGlobalEntity, a);
			Assert.assertEquals(testEntity, b);

			cacheClient.globalRemoveByPk(globalId, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
			cacheClient.removeByPk(shardingId, sk, TestEntity.class);
		} catch (Exception e) {
			cacheClient.rollback();
		}
	}

	@Test
	public void testRollback() {
		long globalId = 1;
		long shardingId = 1;
		TestGlobalEntity testGlobalEntity = createGlobalEntity();
		testGlobalEntity.setId(globalId);
		TestEntity testEntity = createEntity();
		testEntity.setId(shardingId);

		cacheClient.beginTransaction();
		try {
			globalId = cacheClient.globalSave(testGlobalEntity).longValue();
			shardingId = cacheClient.save(testEntity).longValue();

			throw new RuntimeException();
		} catch (Exception e) {
			cacheClient.rollback();
		}

		TestGlobalEntity a = cacheClient.findGlobalByPk(globalId, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		IShardingKey<Integer> sk = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, testEntity.getTestInt());
		TestEntity b = cacheClient.findByPk(shardingId, sk, TestEntity.class);

		Assert.assertNull(a);
		Assert.assertNull(b);
	}

}
