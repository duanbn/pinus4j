package org.pinus4j.datalayer;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pinus4j.ApiBaseTest;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.datalayer.iterator.ShardingRecordIterator;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.exceptions.DBClusterException;

public class ShardingRecrodIteratorTest extends ApiBaseTest {

	private Number[] pks;

	private IShardingKey<Integer> moreKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);

	private IRecordIterator<TestEntity> reader;

	private List<TestEntity> entities;

	private static final int SIZE = 2100;

	private ShardingDBResource dbResource;

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

		IDBCluster dbCluster = cacheClient.getDBCluster();
		try {
			dbResource = (ShardingDBResource) dbCluster.selectDBResourceFromMaster("test_entity", moreKey);
		} catch (DBClusterException e) {
			e.printStackTrace();
		}
		this.reader = new ShardingRecordIterator<TestEntity>(dbResource, TestEntity.class);
	}

	@After
	public void after() {
		// remove more
		cacheClient.removeByPks(moreKey, TestEntity.class, pks);
		dbResource.close();
	}

	@Test
	public void testCount() {
		Assert.assertEquals(SIZE, reader.getCount());
	}

	@Test
	public void testIt() {
		TestEntity entity = null;
		int i = 0;
		while (this.reader.hasNext()) {
			entity = this.reader.next();
			Assert.assertEquals(this.entities.get(i++), entity);
		}
	}

}
