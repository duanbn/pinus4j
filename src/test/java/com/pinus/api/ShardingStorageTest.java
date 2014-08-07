package com.pinus.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.entity.TestEntity;

public class ShardingStorageTest extends BaseTest {

	private Number pk1;
	private Number pk2;
	private Number[] pks;

	private IShardingKey<Integer> moreKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);

	@Before
	public void before() {
		// save one
		TestEntity entity = createEntity();
		entity.setTestString("i am pinus1");
		pk1 = cacheClient.save(entity);
		entity = createEntity();
		entity.setTestString("i am pinus2");
		pk2 = cacheClient.save(entity);
		// check save one
		IShardingKey<Number> oneKey = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk1);
		entity = cacheClient.findByPk(pk1, oneKey, TestEntity.class);
		Assert.assertNotNull(entity);

		// save more
		List<TestEntity> entities = new ArrayList<TestEntity>(5);
		for (int i = 0; i < 5; i++) {
			entity = createEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		pks = cacheClient.saveBatch(entities, moreKey);
		// check save more
		entities = cacheClient.findByPks(moreKey, TestEntity.class, pks);
		Assert.assertEquals(5, entities.size());
	}

	@Test
	public void testGetCountClass() {
		int count = cacheClient.getCount(TestEntity.class).intValue();
		Assert.assertEquals(7, count);
	}

	@Test
	public void testGetCountClassQuery() {
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "i am pinus"));
		int count = cacheClient.getCount(TestEntity.class, query).intValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountQueryShardingClass() {
		int count = cacheClient.getCount(moreKey, TestEntity.class).intValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountShardingClass() {
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "i am pinus"));
		int count = cacheClient.getCount(query, moreKey, TestEntity.class).intValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testFindByQueryQueryShardingKeyClass() {
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "i am pinus"));
		List<TestEntity> entities = cacheClient.findByQuery(query, moreKey, TestEntity.class);
		Assert.assertEquals(5, entities.size());

		query.setFields("testString");
		entities = cacheClient.findByQuery(query, moreKey, TestEntity.class);
		for (TestEntity entity : entities) {
			Assert.assertEquals("i am pinus", entity.getTestString());
			Assert.assertEquals(0, entity.getTestInt());
		}
	}

	@Test
	public void testFindByShardingPairListClassNumber() {
		List<IShardingKey<?>> keys = new ArrayList<IShardingKey<?>>();
		keys.add(new ShardingKey(CLUSTER_KLSTORAGE, pk1));
		keys.add(new ShardingKey(CLUSTER_KLSTORAGE, pk2));
		List<TestEntity> entities = cacheClient.findByShardingPair(keys, TestEntity.class, pk1, pk2);
		Assert.assertEquals(2, entities.size());
	}

	@Test
	public void testUpdateObject() {
		IShardingKey<Number> oneKey = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk1);
		TestEntity entity = cacheClient.findByPk(pk1, oneKey, TestEntity.class);
		entity.setTestFloat(1.1f);
		cacheClient.update(entity);
		TestEntity after = cacheClient.findByPk(pk1, oneKey, TestEntity.class);
		Assert.assertEquals(entity, after);
	}

	@Test
	public void testUpdateBatchListShardingKey() {
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "i am pinus"));
		List<TestEntity> entities = cacheClient.findByQuery(query, moreKey, TestEntity.class);
		for (TestEntity entity : entities) {
			entity.setTestFloat(1.1f);
		}
		cacheClient.updateBatch(entities, moreKey);
		List<TestEntity> after = cacheClient.findByQuery(query, moreKey, TestEntity.class);
		for (int i = 0; i < entities.size(); i++) {
			Assert.assertEquals(entities.get(i), after.get(i));
		}
	}

	@After
	public void after() {
		// remove one
		IShardingKey<Number> oneKey = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk1);
		cacheClient.removeByPk(pk1, oneKey, TestEntity.class);
		oneKey = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk2);
		cacheClient.removeByPk(pk2, oneKey, TestEntity.class);
		// check remove one
		TestEntity entity = cacheClient.findByPk(pk1, oneKey, TestEntity.class);
		Assert.assertNull(entity);

		// remove more
		cacheClient.removeByPks(moreKey, TestEntity.class, pks);
		// check remove more
		List<TestEntity> entities = cacheClient.findByPks(moreKey, TestEntity.class, pks);
		Assert.assertEquals(0, entities.size());
	}

}
