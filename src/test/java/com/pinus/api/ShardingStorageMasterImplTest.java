package com.pinus.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.entity.TestEntity;
import com.pinus.BaseTest;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.api.query.Order;

public class ShardingStorageMasterImplTest extends BaseTest {

	@Test
	public void testGetCount() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 1);
		long count = this.client.getCount(shardingValue, TestEntity.class).longValue();
		System.out.println("count=" + count);
	}

	@Test
	public void testSave() throws Exception {
		TestEntity entity = createEntity();
		this.client.save(entity).longValue();
	}

	@Test
	public void testSaveBatch() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 2);
		List<TestEntity> testEntitys = new ArrayList<TestEntity>();
		for (int i = 0; i < 10; i++) {
			testEntitys.add(createEntity());
		}
		this.client.saveBatch(testEntitys, shardingValue);
	}

	@Test
	public void testUpdate() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 2);

		IQuery query = this.client.createQuery();
		query.add(Condition.lt("testInt", 0));
		List<TestEntity> entitys = this.client.findByQuery(query, shardingValue, TestEntity.class);

		if (!entitys.isEmpty()) {
			TestEntity entity = entitys.get(0);
			entity.setTestInt(r.nextInt());
			this.client.update(entity);
		}
	}

	@Test
	public void testUpdateBatch() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 2);
		IQuery query = this.client.createQuery();
		query.add(Condition.lt("testByte", 0));
		List<TestEntity> entitys = this.client.findByQuery(query, shardingValue, TestEntity.class);
		for (TestEntity entity : entitys) {
			entity.setTestInt(r.nextInt());
		}
		this.client.updateBatch(entitys, shardingValue);
	}

	@Test
	public void testRemoveByPk() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 2);
		List<TestEntity> entitys = this.client.findMore(shardingValue, TestEntity.class, 0, 10);
		if (!entitys.isEmpty())
			this.client.removeByPk(entitys.get(0).getTestId(), shardingValue, TestEntity.class);
	}

	@Test
	public void testRemoveByPks() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 2);
		List<TestEntity> entitys = this.client.findMore(shardingValue, TestEntity.class, 0, 20);
		Number[] pks = new Number[3];
		for (int i = 0; i < 3; i++) {
			pks[i] = entitys.get(i).getTestId();
		}
		this.client.removeByPks(pks, shardingValue, TestEntity.class);
	}

	@Test
	public void testQuery() throws Exception {
		IShardingValue<?> shardingValue = new ShardingValue<Integer>(CLUSTER_NAME, 2);
		int count = this.client.getCount(shardingValue, TestEntity.class).intValue();
		System.out.println(count);
		List<TestEntity> entitys = this.client.findMore(shardingValue, TestEntity.class, 0, 10);
		TestEntity one = this.client.findByPk(entitys.get(0).getTestId(), shardingValue, TestEntity.class);
		Assert.assertNotNull(one);
		List<Long> pks = new ArrayList<Long>();
		for (TestEntity entity : entitys) {
			pks.add(entity.getTestId());
		}
		List<TestEntity> entitys1 = this.client.findByPkList(pks, shardingValue, TestEntity.class);
		Assert.assertEquals(entitys.size(), entitys1.size());

		IQuery query = this.client.createQuery();
		query.add(Condition.lt("testInt", 100000));
		query.add(Condition.gte("testLong", 0));
		query.add(Condition.in("testBool", true, false));
		query.orderBy("testId", Order.DESC);
		List<TestEntity> entitys2 = this.client.findByQuery(query, shardingValue, TestEntity.class);
		System.out.println(entitys2.size());

		SQL<TestEntity> sql = new SQL<TestEntity>(TestEntity.class,
				"select * from test_entity where (testInt > ? AND testLong < ?) OR (testBool = ? AND testShort > ?)");
		sql.setParams(new Object[] { 0, 1000, '1', 50 });
		List<TestEntity> entitys3 = this.client.findBySql(sql, shardingValue);
		System.out.println(entitys3.size());
	}

}
