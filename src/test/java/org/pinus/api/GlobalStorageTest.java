package org.pinus.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.entity.TestGlobalEntity;

public class GlobalStorageTest extends BaseTest {

	private Number pk1;
	private Number[] pks;

	@Before
	public void before() {
		// save global one
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("i am pinus");
		pk1 = cacheClient.globalSave(entity);
		entity.setId(pk1.longValue());
		// check save one
		TestGlobalEntity entity1 = cacheClient.findGlobalByPk(pk1, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(entity, entity1);

		// save more
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		for (int i = 0; i < 5; i++) {
			entity = createGlobalEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		entities.get(0).setId(r.nextInt(100000000));
		pks = cacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);
		for (int i = 0; i < entities.size(); i++) {
			entities.get(i).setId(pks[i].longValue());
		}

		// check save more
		List<TestGlobalEntity> entities1 = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (int i = 0; i < entities.size(); i++) {
			Assert.assertEquals(entities.get(i), entities1.get(i));
		}
	}

	@Test
	public void testGetGlobalCountStringClass() {
		int count = cacheClient.getGlobalCount(CLUSTER_KLSTORAGE, TestGlobalEntity.class).intValue();
		Assert.assertEquals(6, count);
	}

	@Test
	public void testFindGlobalByQueryQueryStringClass() {
		IQuery query = cacheClient.createQuery();
		query.add(Condition.eq("testString", "i am pinus"));
		List<TestGlobalEntity> entities = cacheClient.findGlobalByQuery(query, CLUSTER_KLSTORAGE,
				TestGlobalEntity.class);
		Assert.assertEquals(6, entities.size());

		query.setFields("testString");
		entities = cacheClient.findGlobalByQuery(query, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		for (TestGlobalEntity entity : entities) {
			Assert.assertEquals("i am pinus", entity.getTestString());
			Assert.assertEquals(0, entity.getTestInt());
			Assert.assertEquals(0.0f, entity.getTestFloat());
			Assert.assertEquals(0.0, entity.getTestDouble());
		}
	}

	@Test
	public void testFindGlobalBySqlSqlString() {
		SQL sql = SQL.valueOf("select * from testglobalentity where testString=?", "i am pinus");
		List<Map<String, Object>> rst = cacheClient.findGlobalBySql(sql, CLUSTER_KLSTORAGE);
		Assert.assertEquals(6, rst.size());
		for (Map<String, Object> map : rst) {
			Assert.assertEquals("i am pinus", map.get("testString"));
		}
	}

	@Test
	public void testGlobalUpdateObject() {
		TestGlobalEntity entity = cacheClient.findGlobalByPk(pk1, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		entity.setTestFloat(0.0f);
		cacheClient.globalUpdate(entity);
		TestGlobalEntity after = cacheClient.findGlobalByPk(pk1, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertEquals(entity, after);
	}

	@Test
	public void testGlobalUpdateBatchListString() {
		List<TestGlobalEntity> entities = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (TestGlobalEntity entity : entities) {
			entity.setTestFloat(0.0f);
		}
		cacheClient.globalUpdateBatch(entities, CLUSTER_KLSTORAGE);
		List<TestGlobalEntity> after = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		for (int i = 0; i < 5; i++) {
			Assert.assertEquals(entities.get(i), after.get(i));
		}
	}

	@After
	public void after() {
		// remove one
		cacheClient.globalRemoveByPk(pk1, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		// check remove one
		TestGlobalEntity entity = cacheClient.findGlobalByPk(pk1, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNull(entity);

		// remove more
		cacheClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		// check remove more
		List<TestGlobalEntity> entities = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		Assert.assertEquals(0, entities.size());
	}

}
