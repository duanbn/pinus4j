package org.pinus.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.entity.TestEntity;

public class ShardingTaskTest extends BaseTest {

	private Number[] pks;

	private IShardingKey<Integer> moreKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);

	private List<TestEntity> entities;

	private static final int SIZE = 2100;

	@Before
	public void before() {
		// save more
		entities = new ArrayList<TestEntity>(SIZE);
		TestEntity entity = null;
		for (int i = 0; i < SIZE; i++) {
			entity = createEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		pks = cacheClient.saveBatch(entities, moreKey);
		// check save more
		entities = cacheClient.findByPks(moreKey, TestEntity.class, pks);
		Assert.assertEquals(SIZE, entities.size());
	}

	@After
	public void after() {
		// remove more
		cacheClient.removeByPks(moreKey, TestEntity.class, pks);
	}

	@Test
	public void testSubmit() throws InterruptedException {
		ITask<TestEntity> task = new SimpleShardingTask();

		TaskFuture future = cacheClient.submit(task, TestEntity.class);
		while (!future.isDone()) {
			System.out.println(future.getProgress());

			Thread.sleep(2000);
		}

		System.out.println(future);
	}

	@Test
	public void testSubmitQuery() throws InterruptedException {
		ITask<TestEntity> task = new SimpleShardingTask();
		IQuery query = cacheClient.createQuery();
		query.add(Condition.gt("testInt", 100));

		TaskFuture future = cacheClient.submit(task, TestEntity.class, query);
		future.await();

		System.out.println(future);
	}

	private static final Random r = new Random();

	public static class SimpleShardingTask implements ITask<TestEntity> {
		@Override
		public void doTask(List<TestEntity> entity) {
			try {
				Thread.sleep(r.nextInt(10));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
