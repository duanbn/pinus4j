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

public class GlobalTaskTest extends BaseTest {

	private Number[] pks;

	private List<TestGlobalEntity> entities;

	private static final int SIZE = 2100;

	@Before
	public void before() {
		// save more
		entities = new ArrayList<TestGlobalEntity>(SIZE);
		TestGlobalEntity entity = null;
		for (int i = 0; i < SIZE; i++) {
			entity = createGlobalEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		pks = cacheClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);
		// check save more
		entities = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE,
				TestGlobalEntity.class, pks);
		Assert.assertEquals(SIZE, entities.size());
	}

	@After
	public void after() {
		// remove more
		cacheClient.globalRemoveByPks(CLUSTER_KLSTORAGE,
				TestGlobalEntity.class, pks);
	}

	@Test
	public void testSubmit() throws InterruptedException {
		ITask<TestGlobalEntity> task = new SimpleGlobalTask();

		TaskFuture future = cacheClient.submit(task, TestGlobalEntity.class);
		future.await();

		System.out.println(future.getCollector().get("testInt"));

		System.out.println(future);
	}

	@Test
	public void testSubmitQuery() throws InterruptedException {
		ITask<TestGlobalEntity> task = new SimpleGlobalTask();
		IQuery query = cacheClient.createQuery();
		query.add(Condition.gt("testInt", 100));

		TaskFuture future = cacheClient.submit(task, TestGlobalEntity.class,
				query);
		future.await();

		System.out.println(future);
	}

	public static class SimpleGlobalTask implements ITask<TestGlobalEntity> {
		@Override
		public void doTask(List<TestGlobalEntity> entityList,
				TaskCollector collector) {
			for (TestGlobalEntity entity : entityList) {
				collector.incr("testInt", entity.getTestInt());
			}
		}
	}

}
