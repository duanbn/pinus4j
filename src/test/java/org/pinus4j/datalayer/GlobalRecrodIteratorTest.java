package org.pinus4j.datalayer;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pinus4j.ApiBaseTest;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.resources.GlobalDBResource;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.datalayer.iterator.GlobalRecordIterator;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.utils.ReflectUtil;

public class GlobalRecrodIteratorTest extends ApiBaseTest {

	private Number[] pks;

	private IRecordIterator<TestGlobalEntity> reader;

	private List<TestGlobalEntity> entities;

	private static final int SIZE = 2100;

	private IDBResource dbResource = null;

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
		entities = cacheClient.findGlobalByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		Assert.assertEquals(SIZE, entities.size());

		IDBCluster dbCluster = cacheClient.getDBCluster();
		try {
			dbResource = dbCluster.getMasterGlobalDBResource(CLUSTER_KLSTORAGE,
					ReflectUtil.getTableName(TestGlobalEntity.class));
		} catch (DBClusterException e) {
			e.printStackTrace();
		}
		this.reader = new GlobalRecordIterator<TestGlobalEntity>((GlobalDBResource) dbResource, TestGlobalEntity.class);
	}

	@After
	public void after() {
		// remove more
		cacheClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, pks);
		dbResource.close();
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
