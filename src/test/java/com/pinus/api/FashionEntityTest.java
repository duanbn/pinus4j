package com.pinus.api;

import junit.framework.Assert;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.entity.TestEntity;
import com.pinus.entity.TestGlobalEntity;

public class FashionEntityTest extends BaseTest {

	@Test
	public void testSharding() {
		TestEntity entity = createEntity();
		Number pk = entity.save();

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		entity = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertNotNull(entity);

		entity.setTestInt(100);
		entity.update();
		TestEntity after = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertEquals(entity, after);

		after.remove();
		after = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertNull(after);
	}

	@Test
	public void testGlobal() {
		TestGlobalEntity entity = createGlobalEntity();

		Number pk = entity.save();

		entity = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNotNull(entity);

		entity.setTestInt(100);
		entity.update();
		TestGlobalEntity after = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(entity, after);

		after.remove();
		after = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(after);
	}

}
