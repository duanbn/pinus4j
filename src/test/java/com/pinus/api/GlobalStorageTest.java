package com.pinus.api;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.entity.TestGlobalEntity;

public class GlobalStorageTest extends BaseTest {

	private Number pk1;
	private Number[] pks;

	@Before
	public void before() {
		// save global one
		TestGlobalEntity entity = createGlobalEntity();
		entity.setTestString("i am pinus");
		pk1 = cacheClient.globalSave(entity);
		// check save one
		entity = cacheClient.findGlobalByPk(pk1, CLUSTER_KLSTORAGE, TestGlobalEntity.class);
		Assert.assertNotNull(entity);

		// save more
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		for (int i = 0; i < 5; i++) {
			entity = createGlobalEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		pks = cacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);
		// check save more
		entities = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		Assert.assertEquals(5, entities.size());
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
