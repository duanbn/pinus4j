package org.pinus4j.cache;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.IShardingStorageClient;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.exceptions.DBClusterException;

public class MemCachedSecondCacheTest extends BaseTest {

	private static IQuery query;
	private static ShardingDBResource db;

	private static IShardingStorageClient storageClient;
	private static ISecondCache secondCache;

	@BeforeClass
	public static void before() {
		storageClient = getStorageClient();
		secondCache = storageClient.getDBCluster().getSecondCache();

		query = storageClient.createQuery();
		query.add(Condition.eq("aa", "aa"));
		query.add(Condition.eq("bb", "bb"));

		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);
		try {
			db = (ShardingDBResource) storageClient.getDBCluster().selectDBResourceFromMaster("test_entity",
					shardingValue);
		} catch (DBClusterException e) {
			e.printStackTrace();
		}

		List<String> data = new ArrayList<String>();
		data.add("aaa");
		data.add("bbb");
		secondCache.putGlobal(query.getWhereSql(), CLUSTER_KLSTORAGE, "testglobalentity", data);

		data = new ArrayList<String>();
		data.add("ccc");
		data.add("ddd");
		secondCache.put(query.getWhereSql(), db, data);
	}

	@AfterClass
	public static void after() {
		secondCache.removeGlobal(CLUSTER_KLSTORAGE, "testglobalentity");
		secondCache.remove(db);

		storageClient.destroy();
	}

	@Test
	public void testGetGlobal() {
		List<String> data = (List<String>) secondCache.getGlobal(query.getWhereSql(), CLUSTER_KLSTORAGE,
				"testglobalentity");
		Assert.assertEquals("aaa", data.get(0));
		Assert.assertEquals("bbb", data.get(1));
	}

	@Test
	public void testGet() {
		List<String> data = (List<String>) secondCache.get(query.getWhereSql(), db);
		Assert.assertEquals("ccc", data.get(0));
		Assert.assertEquals("ddd", data.get(1));
	}

}
