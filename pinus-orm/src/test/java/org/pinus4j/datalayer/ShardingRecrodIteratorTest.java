package org.pinus4j.datalayer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.IShardingStorageClient;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.datalayer.iterator.ShardingRecordIterator;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.exceptions.DBClusterException;

public class ShardingRecrodIteratorTest extends BaseTest {

	private static Number[] pks;

	private static IShardingKey<Integer> moreKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);

	private static IRecordIterator<TestEntity> reader;

	private static List<TestEntity> entities;

	private static final int SIZE = 2100;

	private static ShardingDBResource dbResource;

	private static IShardingStorageClient storageClient;

	@BeforeClass
	public void before() {
		storageClient = getStorageClient();

		// save more
		entities = new ArrayList<TestEntity>(SIZE);
		TestEntity entity = null;
		for (int i = 0; i < SIZE; i++) {
			entity = createEntity();
			entity.setTestString("i am pinus");
			entities.add(entity);
		}
		pks = storageClient.saveBatch(entities, moreKey);
		// check save more
		entities = storageClient.findByPkList(Arrays.asList(pks), moreKey, TestEntity.class);
		Assert.assertEquals(SIZE, entities.size());

		IDBCluster dbCluster = storageClient.getDBCluster();
		try {
			dbResource = (ShardingDBResource) dbCluster.selectDBResourceFromMaster("test_entity", moreKey);
		} catch (DBClusterException e) {
			e.printStackTrace();
		}
		this.reader = new ShardingRecordIterator<TestEntity>(dbResource, TestEntity.class);
	}

	@AfterClass
	public void after() {
		// remove more
		storageClient.removeByPks(moreKey, TestEntity.class, pks);
		dbResource.close();

		storageClient.destroy();
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
