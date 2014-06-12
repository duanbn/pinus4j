package com.pinus.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.TestEntity;
import com.pinus.TestGlobalEntity;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;

public class NoCacheShardingStorageClientTest extends BaseTest {

	@Test
	public void testGetCount() {
		long count = noCacheClient.getCount(TestEntity.class).longValue();
		System.out.println(count);
	}

	@Test
	public void testGlobalSave() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("my name is duanbingnan");
		Number pk = noCacheClient.globalSave(entity);

		TestGlobalEntity entityFromDB = noCacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(entity, entityFromDB);

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		entityFromDB = noCacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testGlobalUpdate() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("my author name is duanbingnan");
		Number pk = noCacheClient.globalSave(entity);

		TestGlobalEntity before = noCacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		before.setTestString("my name is pinus");
		noCacheClient.globalUpdate(before);
		TestGlobalEntity after = noCacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(before, after);

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		TestGlobalEntity entityFromDB = noCacheClient.findGlobalByPk(pk, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testGlobalSaveBatch() {
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		TestGlobalEntity entity1 = createGlobalEntity();
		TestGlobalEntity entity2 = createGlobalEntity();
		entities.add(entity1);
		entities.add(entity2);

		Number[] pks = noCacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);

		List<TestGlobalEntity> entitiesFromDB = noCacheClient.findGlobalByPks(CLUSTER_KLSTORAGE,
				TestGlobalEntity.class, pks);
		for (int i = 0; i < entitiesFromDB.size(); i++) {
			Assert.assertEquals(entities.get(i), entitiesFromDB.get(i));
		}

		noCacheClient.globalRemoveByPks(pks, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		entitiesFromDB = noCacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testGlobalUpdateBatch() {
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		TestGlobalEntity entity1 = createGlobalEntity();
		TestGlobalEntity entity2 = createGlobalEntity();
		entities.add(entity1);
		entities.add(entity2);

		Number[] pks = noCacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);

		List<TestGlobalEntity> before = noCacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (TestGlobalEntity entity : before) {
			entity.setTestString("my name is pinus");
		}
		noCacheClient.globalUpdateBatch(before, CLUSTER_KLSTORAGE);
		List<TestGlobalEntity> after = noCacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (int i = 0; i < after.size(); i++) {
			Assert.assertEquals(before.get(i), after.get(i));
		}

		noCacheClient.globalRemoveByPks(pks, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		List<TestGlobalEntity> entitiesFromDB = noCacheClient.findGlobalByPks(CLUSTER_KLSTORAGE,
				TestGlobalEntity.class, pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testSave() {
		TestEntity entity = createEntity();
		Number pk = noCacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		TestEntity entityFromDB = noCacheClient.findByPk(pk, key, TestEntity.class);

		Assert.assertEquals(entity, entityFromDB);

		noCacheClient.removeByPk(pk, key, TestEntity.class);
		entityFromDB = noCacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertNull(entityFromDB);
	}

	@Test
	public void testUpdate() {
		TestEntity entity = createEntity();
		Number pk = noCacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		TestEntity before = noCacheClient.findByPk(pk, key, TestEntity.class);
		before.setTestString("this way is test update");
		noCacheClient.update(before);
		TestEntity after = noCacheClient.findByPk(pk, key, TestEntity.class);
		Assert.assertEquals(before, after);

		noCacheClient.removeByPk(pk, key, TestEntity.class);
		TestEntity entityFromDB = noCacheClient.findByPk(pk, key, TestEntity.class);
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
		Number[] pks = noCacheClient.saveBatch(entities, key);

		List<TestEntity> entitiesFromDB = noCacheClient.findByPks(key, TestEntity.class, pks);
		for (int i = 0; i < entitiesFromDB.size(); i++) {
			Assert.assertEquals(entities.get(i), entitiesFromDB.get(i));
		}

		noCacheClient.removeByPks(pks, key, TestEntity.class);
		entitiesFromDB = noCacheClient.findByPks(key, TestEntity.class, pks);
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
		Number[] pks = noCacheClient.saveBatch(entities, key);

		List<TestEntity> before = noCacheClient.findByPks(key, TestEntity.class, pks);
		for (TestEntity entity : before) {
			entity.setTestDate(new Date());
		}
		noCacheClient.updateBatch(before, key);
		List<TestEntity> after = noCacheClient.findByPks(key, TestEntity.class, pks);
		for (int i = 0; i < after.size(); i++) {
			Assert.assertEquals(before.get(i), after.get(i));
		}

		noCacheClient.removeByPks(pks, key, TestEntity.class);
		List<TestEntity> entitiesFromDB = noCacheClient.findByPks(key, TestEntity.class, pks);
		Assert.assertTrue(entitiesFromDB.isEmpty());
	}

	@Test
	public void testFindGlobalOneByQuery() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("this method is test find global one by query");
		Number pk = noCacheClient.globalSave(entity);

		IQuery query = noCacheClient.createQuery();
		query.add(Condition.eq("testString", "this method is test find global one by query"));
		TestGlobalEntity entityFromDB = noCacheClient.findGlobalOneByQuery(query, CLUSTER_KLSTORAGE,
				TestGlobalEntity.class);
		Assert.assertEquals(entity, entityFromDB);

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testFindOneByQuery() {
		TestEntity entity = createEntity();
		entity.setTestString("this method is test find one by query");
		Number pk = noCacheClient.save(entity);

		IQuery query = noCacheClient.createQuery();
		query.add(Condition.eq("testString", "this method is test find one by query"));
		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		TestEntity entityFromDB = noCacheClient.findOneByQuery(query, key, TestEntity.class);
		Assert.assertEquals(entity, entityFromDB);

		noCacheClient.removeByPk(pk, key, TestEntity.class);
	}

	@Test
	public void testGetGlobalCountStringClassOfQ() {
		TestGlobalEntity entity = createGlobalEntity();
		Number pk = noCacheClient.globalSave(entity);

		Number count = noCacheClient.getGlobalCount(CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(1, count.intValue());

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testGetGlobalCountStringSQLOfQ() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("this methid is test global count by sql");
		Number pk = noCacheClient.globalSave(entity);

		String s = "select count(*) from testglobalentity where teststring=?";
		SQL<TestGlobalEntity> sql = new SQL<TestGlobalEntity>(TestGlobalEntity.class, s,
				new Object[] { "this methid is test global count by sql" });
		Number count = noCacheClient.getGlobalCount(CLUSTER_KLSTORAGE, sql);
		Assert.assertEquals(1, count.intValue());

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testFindGlobalBySql() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("this methid is test find global by sql");
		Number pk = noCacheClient.globalSave(entity);

		String s = "select * from testglobalentity where teststring=?";
		SQL<TestGlobalEntity> sql = new SQL<TestGlobalEntity>(TestGlobalEntity.class, s);
		sql.setParams(new Object[] { "this methid is test find global by sql" });
		List<TestGlobalEntity> entitiesFromDB = noCacheClient.findGlobalBySql(sql, CLUSTER_KLSTORAGE);
		Assert.assertEquals(1, entitiesFromDB.size());
		Assert.assertEquals(entity, entitiesFromDB.get(0));

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testFindGlobalByQuery() {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("this methid is test find global by query");
		Number pk = noCacheClient.globalSave(entity);

		IQuery query = noCacheClient.createQuery();
		query.add(Condition.eq("testString", "this methid is test find global by query"));
		List<TestGlobalEntity> entitiesFromDB = noCacheClient.findGlobalByQuery(query, CLUSTER_KLSTORAGE,
				TestGlobalEntity.class);
		Assert.assertEquals(1, entitiesFromDB.size());
		Assert.assertEquals(entity, entitiesFromDB.get(0));

		noCacheClient.globalRemoveByPk(pk, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
	}

	@Test
	public void testGetCountIShardingKeyOfQClassOfQ() {
		TestEntity entity = createEntity();
		Number pk = noCacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		Number count = noCacheClient.getCount(key, TestEntity.class);
		Assert.assertEquals(1, count.intValue());

		noCacheClient.removeByPk(pk, key, TestEntity.class);
	}

	@Test
	public void testGetCountIShardingKeyOfQSQLOfQ() {
		TestEntity entity = createEntity();
		entity.setTestString("this method is test count by sql");
		Number pk = noCacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		String s = "select count(*) from test_entity where testString=?";
		SQL<TestEntity> sql = new SQL<TestEntity>(TestEntity.class, s,
				new Object[] { "this method is test count by sql" });
		Number count = noCacheClient.getCount(key, sql);
		Assert.assertEquals(1, count.intValue());

		noCacheClient.removeByPk(pk, key, TestEntity.class);
	}

	@Test
	public void testFindByShardingPairListOfQextendsNumberListOfIShardingKeyOfQClassOfT() {
		List<Number> pkList = new ArrayList<Number>();

		TestEntity entity1 = createEntity();
		TestEntity entity2 = createEntity();
		pkList.add(noCacheClient.save(entity1));
		pkList.add(noCacheClient.save(entity2));

		List<IShardingKey<?>> keys = new ArrayList<IShardingKey<?>>();
		for (Number pk : pkList) {
			keys.add(new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk));
		}
		List<TestEntity> entities = noCacheClient.findByShardingPair(pkList, keys, TestEntity.class);

		Assert.assertEquals(entity1, entities.get(0));
		Assert.assertEquals(entity2, entities.get(1));

		for (IShardingKey<?> key : keys) {
			noCacheClient.removeByPk((Number) key.getValue(), key, TestEntity.class);
		}
	}

	@Test
	public void testFindBySql() {
		TestEntity entity = createEntity();
		entity.setTestString("this method is test find by sql");
		Number pk = noCacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		String s = "select * from test_entity where testString=?";
		SQL<TestEntity> sql = new SQL<TestEntity>(TestEntity.class, s,
				new Object[] { "this method is test find by sql" });
		List<TestEntity> entitiesFromDB = noCacheClient.findBySql(sql, key);
		Assert.assertEquals(1, entitiesFromDB.size());
		Assert.assertEquals(entity, entitiesFromDB.get(0));

		noCacheClient.removeByPk(pk, key, TestEntity.class);
	}

	@Test
	public void testFindByQuery() {
		TestEntity entity = createEntity();
		entity.setTestString("this method is test find by query");
		Number pk = noCacheClient.save(entity);

		IShardingKey<Number> key = new ShardingKey<Number>(CLUSTER_KLSTORAGE, pk);
		IQuery query = noCacheClient.createQuery();
		query.add(Condition.eq("testString", "this method is test find by query"));
		List<TestEntity> entitiesFromDB = noCacheClient.findByQuery(query, key, TestEntity.class);
		Assert.assertEquals(1, entitiesFromDB.size());
		Assert.assertEquals(entity, entitiesFromDB.get(0));

		noCacheClient.removeByPk(pk, key, TestEntity.class);
	}

}
