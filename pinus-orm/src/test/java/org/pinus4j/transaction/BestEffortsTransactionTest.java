package org.pinus4j.transaction;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.IShardingStorageClient;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class BestEffortsTransactionTest extends BaseTest {

	private static IShardingStorageClient storageClient;

	@BeforeClass
	public static void before() {
		storageClient = getStorageClient();
	}

	@AfterClass
	public static void after() {
		storageClient.destroy();
	}

	@Test
	public void testCommit() {
		TestGlobalEntity testGlobalEntity = createGlobalEntity();
		TestEntity testEntity = createEntity();

		storageClient.beginTransaction();
		try {
			long globalId = storageClient.globalSave(testGlobalEntity).longValue();
			long shardingId = storageClient.save(testEntity).longValue();

			storageClient.commit();

			TestGlobalEntity a = storageClient.findByPk(globalId, TestGlobalEntity.class);
			IShardingKey<Integer> sk = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, testEntity.getTestInt());
			TestEntity b = storageClient.findByPk(shardingId, sk, TestEntity.class);
			Assert.assertEquals(testGlobalEntity, a);
			Assert.assertEquals(testEntity, b);

			storageClient.globalRemoveByPk(globalId, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
			storageClient.removeByPk(shardingId, sk, TestEntity.class);
		} catch (Exception e) {
			storageClient.rollback();
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

		storageClient.beginTransaction();
		try {
			storageClient.globalSave(testGlobalEntity).longValue();
			storageClient.save(testEntity).longValue();

			throw new RuntimeException();
		} catch (Exception e) {
			storageClient.rollback();
		}

		TestGlobalEntity a = storageClient.findByPk(globalId, TestGlobalEntity.class);
		IShardingKey<Integer> sk = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, testEntity.getTestInt());
		TestEntity b = storageClient.findByPk(shardingId, sk, TestEntity.class);

		Assert.assertNull(a);
		Assert.assertNull(b);
	}

}
