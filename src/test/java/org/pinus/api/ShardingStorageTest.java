package org.pinus.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus.api.IShardingKey;
import org.pinus.api.SQL;
import org.pinus.api.ShardingKey;
import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.entity.TestEntity;

public class ShardingStorageTest extends BaseTest {

	private Number pk1;
	private IShardingKey<Number> shardingKey1;

	private Number pk2;
	private IShardingKey<Number> shardingKey2;

	private Number[] pks;

	private IShardingKey<Integer> moreKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);

	@Before
	public void before() {
		// save one
		TestEntity entity = createEntity();
		entity.setTestString("i am pinus1");
		pk1 = cacheClient.save(entity);
		shardingKey1 = new ShardingKey<Number>(CLUSTER_KLSTORAGE, entity.getTestInt());

		entity = createEntity();
		entity.setTestString("i am pinus2");
		pk2 = cacheClient.save(entity);
		shardingKey2 = new ShardingKey<Number>(CLUSTER_KLSTORAGE, entity.getTestInt());

		// check save one
		entity = cacheClient.findByPk(pk1, shardingKey1, TestEntity.class);
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
			Assert.assertEquals(0.0f, entity.getTestFloat());
			Assert.assertEquals(0.0, entity.getTestDouble());
		}
	}

	@Test
	public void testFindBySqlSqlShardingKey() {
		SQL sql = SQL.valueOf("select * from test_entity where testString=?", "i am pinus");
		List<Map<String, Object>> rst = cacheClient.findBySql(sql, moreKey);
		Assert.assertEquals(5, rst.size());
		for (Map<String, Object> map : rst) {
			Assert.assertEquals("i am pinus", map.get("testString"));
		}
	}

	@Test
	public void testUpdateObject() {
		TestEntity entity = cacheClient.findByPk(pk1, shardingKey1, TestEntity.class);
		entity.setTestFloat(1.1f);
		cacheClient.update(entity);
		TestEntity after = cacheClient.findByPk(pk1, shardingKey1, TestEntity.class);
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
		cacheClient.removeByPk(pk1, shardingKey1, TestEntity.class);
		cacheClient.removeByPk(pk2, shardingKey2, TestEntity.class);
		// check remove one
		TestEntity entity = cacheClient.findByPk(pk1, shardingKey1, TestEntity.class);
		Assert.assertNull(entity);

		// remove more
		cacheClient.removeByPks(moreKey, TestEntity.class, pks);
		// check remove more
		List<TestEntity> entities = cacheClient.findByPks(moreKey, TestEntity.class, pks);
		Assert.assertEquals(0, entities.size());
	}

}
