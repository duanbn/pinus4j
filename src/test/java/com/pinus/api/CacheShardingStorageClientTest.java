package com.pinus.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.entity.TestEntity;
import com.pinus.entity.TestGlobalEntity;

public class CacheShardingStorageClientTest extends BaseTest {

	@Test
	public void testGetCount() {
		long count = cacheClient.getCount(TestEntity.class).longValue();
		System.out.println(count);
	}

	@Test
	public void testGetCountQuery() throws Exception {
		IQuery query = cacheClient.createQuery();
		query.add(Condition.lt("id", 999999));
		long count = cacheClient.getCount(TestEntity.class, query).longValue();
		System.out.println(count);
	}

	@Test
	public void testGlobalSave() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("my name is duanbingnan");
		Number pk = cacheClient.globalSave(entity);

		TestGlobalEntity entityFromDB = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(entity, entityFromDB);

		cacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		entityFromDB = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testGlobalUpdate() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("my author name is duanbingnan");
		Number pk = cacheClient.globalSave(entity);

		TestGlobalEntity before = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		before.setTestString("my name is pinus");
		cacheClient.globalUpdate(before);
		TestGlobalEntity after = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(before, after);

		cacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		TestGlobalEntity entityFromDB = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testGlobalSaveBatch() {
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		TestGlobalEntity entity1 = createGlobalEntity();
		TestGlobalEntity entity2 = createGlobalEntity();
		entities.add(entity1);
		entities.add(entity2);

		Number[] pks = cacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);

		List<TestGlobalEntity> entitiesFromDB = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class,
				pks);
		for (int i = 0; i < entitiesFromDB.size(); i++) {
			Assert.assertEquals(entities.get(i), entitiesFromDB.get(i));
		}

		cacheClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		entitiesFromDB = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testGlobalUpdateBatch() {
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		TestGlobalEntity entity1 = createGlobalEntity();
		TestGlobalEntity entity2 = createGlobalEntity();
		entities.add(entity1);
		entities.add(entity2);

		Number[] pks = cacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);

		List<TestGlobalEntity> before = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (TestGlobalEntity entity : before) {
			entity.setTestString("my name is pinus");
		}
		cacheClient.globalUpdateBatch(before, CLUSTER_KLSTORAGE);
		List<TestGlobalEntity> after = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (int i = 0; i < after.size(); i++) {
			Assert.assertEquals(before.get(i), after.get(i));
		}

		cacheClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		List<TestGlobalEntity> entitiesFromDB = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class,
				pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testSave() {
		TestEntity entity = createEntity();
		Number pk = cacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		TestEntity entityFromDB = cacheClient.findByPk(pk, key, TestEntity.class);

		Assert.assertEquals(entity, entityFromDB);

		cacheClient.removeByPk(pk, key, TestEntity.class);
		entityFromDB = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testUpdate() {
		TestEntity entity = createEntity();
		Number pk = cacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		TestEntity before = cacheClient.findByPk(pk, key, TestEntity.class);
		before.setTestString("this way is test update");
		cacheClient.update(before);
		TestEntity after = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertEquals(before, after);

		cacheClient.removeByPk(pk, key, TestEntity.class);
		TestEntity entityFromDB = cacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testSaveBatch() {
		List<TestEntity> entities = new ArrayList<TestEntity>();
		TestEntity entity1 = createEntity();
		TestEntity entity2 = createEntity();
		entities.add(entity1);
		entities.add(entity2);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, 1000001);
		Number[] pks = cacheClient.saveBatch(entities, key);

		List<TestEntity> entitiesFromDB = cacheClient.findByPks(key, TestEntity.class, pks);
		for (int i = 0; i < entitiesFromDB.size(); i++) {
			Assert.assertEquals(entities.get(i), entitiesFromDB.get(i));
		}

		cacheClient.removeByPks(key, TestEntity.class, pks);
		entitiesFromDB = cacheClient.findByPks(key, TestEntity.class, pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testUpdateBatch() {
		List<TestEntity> entities = new ArrayList<TestEntity>();
		TestEntity entity1 = createEntity();
		TestEntity entity2 = createEntity();
		entities.add(entity1);
		entities.add(entity2);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, 1000001);
		Number[] pks = cacheClient.saveBatch(entities, key);

		List<TestEntity> before = cacheClient.findByPks(key, TestEntity.class, pks);
		for (TestEntity entity : before) {
			entity.setTestDate(new Date());
		}
		cacheClient.updateBatch(before, key);
		List<TestEntity> after = cacheClient.findByPks(key, TestEntity.class, pks);
		for (int i = 0; i < after.size(); i++) {
			Assert.assertEquals(before.get(i), after.get(i));
		}

		cacheClient.removeByPks(key, TestEntity.class, pks);
		List<TestEntity> entitiesFromDB = cacheClient.findByPks(key, TestEntity.class, pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testFindGlobalOneByQuery() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("this method is test find global one by query");
		Number pk = cacheClient.globalSave(entity);

		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "this method is test find global one by query"));
		TestGlobalEntity entityFromDB = cacheClient.findGlobalOneByQuery(query, CLUSTER_KLSTORAGE,
				TestGlobalEntity.class);
		Assert.assertEquals(entity, entityFromDB);

		cacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		entity = cacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(entity);
	}

	@Test
	public void testFindOneByQuery() {
		TestEntity entity = createEntity();
		entity.setTestString("this method is test find one by query");
		Number pk = cacheClient.save(entity);

		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "this method is test find one by query"));
		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		TestEntity entityFromDB = cacheClient.findOneByQuery(query, key, TestEntity.class);
		Assert.assertEquals(entity, entityFromDB);

		cacheClient.removeByPk(pk, key, TestEntity.class);
	}

	@Test
	public void testGetGlobalCountStringClassOfQ() {
		TestGlobalEntity entity = createGlobalEntity();
		Number pk = cacheClient.globalSave(entity);

		Number count = cacheClient.getGlobalCount(CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(1, count.intValue());

		cacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testFindGlobalByQuery() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("this methid is test find global by query");
		Number pk = cacheClient.globalSave(entity);

		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "this methid is test find global by query"));
		List<TestGlobalEntity> entitiesFromDB = cacheClient.findGlobalByQuery(query, CLUSTER_KLSTORAGE,
				TestGlobalEntity.class);
		Assert.assertEquals(1, entitiesFromDB.size());
		Assert.assertEquals(entity, entitiesFromDB.get(0));

		cacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testGetCountIShardingKeyOfQClassOfQ() {
		TestEntity entity = createEntity();
		Number pk = cacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		Number count = cacheClient.getCount(key, TestEntity.class);
		Assert.assertEquals(1, count.intValue());

		cacheClient.removeByPk(pk, key, TestEntity.class);
	}

	@Test
	public void testFindByShardingPairListOfQextendsNumberListOfIShardingKeyOfQClassOfT() {
		List<Number> pkList = new ArrayList<Number>();

		TestEntity entity1 = createEntity();
		TestEntity entity2 = createEntity();
		pkList.add(cacheClient.save(entity1));
		pkList.add(cacheClient.save(entity2));

		List<IShardingKey<?>> keys = new ArrayList<IShardingKey<?>>();
		for (Number pk : pkList) {
			keys.add(new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk));
		}
		List<TestEntity> entities = cacheClient.findByShardingPair(pkList, keys, TestEntity.class);

		Assert.assertEquals(entity1, entities.get(0));
		Assert.assertEquals(entity2, entities.get(1));

		for (IShardingKey<?> key : keys) {
			cacheClient.removeByPk((Number) key.getValue(), key, TestEntity.class);
		}
	}

	@Test
	public void testFindByQuery() {
		TestEntity entity = createEntity();
		entity.setTestString("this method is test find by query");
		Number pk = cacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "this method is test find by query"));
		List<TestEntity> entitiesFromDB = cacheClient.findByQuery(query, key, TestEntity.class);
		Assert.assertEquals(1, entitiesFromDB.size());
		Assert.assertEquals(entity, entitiesFromDB.get(0));

		cacheClient.removeByPk(pk, key, TestEntity.class);
	}

}
