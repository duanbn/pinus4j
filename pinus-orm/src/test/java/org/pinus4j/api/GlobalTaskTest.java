package org.pinus4j.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskFuture;

public class GlobalTaskTest extends BaseTest {

	private static Number[] pks;

	private static List<TestGlobalEntity> entities;

	private static final int SIZE = 2100;

	private static IShardingStorageClient storageClient;

	@BeforeClass
	public static void before() {
		storageClient = getStorageClient();

		// save more
		entities = new ArrayList<TestGlobalEntity>(SIZE);
		TestGlobalEntity entity = null;
		for (int i = 0; i < SIZE; i++) {
			entity = createGlobalEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		pks = storageClient.globalSaveBatch(entities, CLUSTER_KLSTORAGE);
		// check save more
		entities = storageClient.findByPkList(Arrays.asList(pks), TestGlobalEntity.class);
		Assert.assertEquals(SIZE, entities.size());
	}

	@AfterClass
	public static void after() {
		// remove more
		storageClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);

		storageClient.destroy();
	}

	@Test
	public void testSubmit() throws InterruptedException {
		ITask<TestGlobalEntity> task = new SimpleGlobalTask();

		TaskFuture future = storageClient.submit(task, TestGlobalEntity.class);
		while (!future.isDone()) {
			System.out.println(future.getProgress());
		}

		System.out.println(future);
	}

	@Test
	public void testSubmitQuery() throws InterruptedException {
		ITask<TestGlobalEntity> task = new SimpleGlobalTask();
		IQuery query = storageClient.createQuery();
		query.add(Condition.gt("testInt", 100));

		TaskFuture future = storageClient.submit(task, TestGlobalEntity.class, query);
		future.await();

		System.out.println(future);
	}

	public static class SimpleGlobalTask extends AbstractTask<TestGlobalEntity> {
		@Override
		public void batchRecord(List<TestGlobalEntity> entityList) {
			for (TestGlobalEntity entity : entityList) {
			}
		}
	}

}
