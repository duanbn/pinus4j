package org.pinus4j.api;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.IShardingKey;
import org.pinus4j.api.ShardingKey;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class FashionEntityTest extends BaseTest {

	@Test
	public void testSharding() {
		TestEntity entity = createEntity();
		entity.setId(100);
		Number pk = entity.save();
		System.out.println(pk);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, entity.getTestInt());
		TestEntity qentity = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertNotNull(qentity);
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("oTestInt", entity.getOTestInt()));
		qentity = cacheClient.findOneByQuery(query, key, TestEntity.class);
		Assert.assertEquals(entity.getOTestInt(), qentity.getOTestInt());

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
