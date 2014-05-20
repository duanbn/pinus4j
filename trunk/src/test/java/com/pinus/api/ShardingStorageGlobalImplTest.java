package com.pinus.api;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.entity.TestGlobalEntity;
import com.pinus.BaseTest;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;

public class ShardingStorageGlobalImplTest extends BaseTest {

	@Test
	public void testCountGlobal() {
		long count = this.client.getGlobalCount(CLUSTER_NAME, TestGlobalEntity.class).longValue();
		System.out.println("count=" + count);
	}

	@Test
	public void testSaveGlobal() {
		TestGlobalEntity entity = createGlobalEntity();
		long id = client.globalSave(entity).longValue();
		System.out.println("new id=" + id);
	}

	@Test
	public void testSaveBatchGlobal() {
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		for (int i = 0; i < 10; i++) {
			entities.add(createGlobalEntity());
		}
		this.client.globalSaveBatch(entities, CLUSTER_NAME);
	}

	@Test
	public void testUpdateBatchGlobal() {
		IQuery query = this.client.createQuery();
		query.add(Condition.eq("testBool", false));
		List<TestGlobalEntity> entities = this.client.findGlobalByQuery(query, CLUSTER_NAME,
				TestGlobalEntity.class);
		for (TestGlobalEntity entity : entities) {
			entity.setTestBool(true);
		}
		this.client.globalUpdateBatch(entities, CLUSTER_NAME);
	}

	@Test
	public void testRemoveByPkGlobal() {
		IQuery query = this.client.createQuery();
		query.add(Condition.gt("testId", 0));
		List<TestGlobalEntity> entities = this.client.findGlobalByQuery(query, CLUSTER_NAME,
				TestGlobalEntity.class);
		this.client.globalRemoveByPk(entities.get(0).getTestId(), TestGlobalEntity.class, CLUSTER_NAME);
		System.out.println("remove id=" + entities.get(0).getTestId());
	}

	@Test
	public void testRemoveByPksGlobal() {
		IQuery query = this.client.createQuery();
		query.add(Condition.gt("testInt", 0));
		List<TestGlobalEntity> entities = this.client.findGlobalByQuery(query, CLUSTER_NAME,
				TestGlobalEntity.class);
		List<Long> ids = new ArrayList<Long>();
		for (TestGlobalEntity entity : entities) {
			ids.add(entity.getTestId());
		}
		this.client.globalRemoveByPks(ids.toArray(new Number[ids.size()]), TestGlobalEntity.class, CLUSTER_NAME);
	}

}
