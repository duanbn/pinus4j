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
import org.pinus4j.cluster.resources.GlobalDBResource;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.datalayer.iterator.GlobalRecordIterator;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.utils.ReflectUtil;

public class GlobalRecrodIteratorTest extends BaseTest {

	private static Number[] pks;

	private static IRecordIterator<TestGlobalEntity> reader;

	private static List<TestGlobalEntity> entities;

	private static final int SIZE = 2100;

	private static IDBResource dbResource = null;

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

		IDBCluster dbCluster = storageClient.getDBCluster();
		try {
			dbResource = dbCluster.getMasterGlobalDBResource(CLUSTER_KLSTORAGE,
					ReflectUtil.getTableName(TestGlobalEntity.class));
		} catch (DBClusterException e) {
			e.printStackTrace();
		}
		reader = new GlobalRecordIterator<TestGlobalEntity>((GlobalDBResource) dbResource, TestGlobalEntity.class);
	}

	@AfterClass
	public void after() {
		// remove more
		storageClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		dbResource.close();

		storageClient.destroy();
	}

	@Test
	public void testCount() {
		Assert.assertEquals(SIZE, reader.getCount());
	}

	@Test
	public void testIt() {
		TestGlobalEntity entity = null;
		int i = 0;
		while (this.reader.hasNext()) {
			entity = this.reader.next();
			Assert.assertEquals(this.entities.get(i++), entity);
		}
	}

}
