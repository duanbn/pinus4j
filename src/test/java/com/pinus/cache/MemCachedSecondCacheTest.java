package com.pinus.cache;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pinus.api.IShardingKey;
import org.pinus.api.ShardingKey;
import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.cluster.DB;
import org.pinus.exception.DBClusterException;

import com.pinus.BaseTest;

public class MemCachedSecondCacheTest extends BaseTest {

	private IQuery query;
	private DB db;

	public MemCachedSecondCacheTest() {
		this.query = cacheClient.createQuery();
		this.query.add(Condition.eq("aa", "aa"));
		this.query.add(Condition.eq("bb", "bb"));
	}

	@Before
	public void before() {
		IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);
		try {
			db = cacheClient.getDbCluster().selectDbFromMaster("test_entity", shardingValue);
		} catch (DBClusterException e) {
			e.printStackTrace();
		}

		List<String> data = new ArrayList<String>();
		data.add("aaa");
		data.add("bbb");
		secondCache.putGlobal(query, CLUSTER_KLSTORAGE, "testglobalentity", data);

		data = new ArrayList<String>();
		data.add("ccc");
		data.add("ddd");
		secondCache.put(query, db, data);
	}

	@After
	public void after() {
		secondCache.removeGlobal(CLUSTER_KLSTORAGE, "testglobalentity");
		secondCache.remove(db);
	}

	@Test
	public void testGetGlobal() {
		List<String> data = (List<String>) secondCache.getGlobal(query, CLUSTER_KLSTORAGE, "testglobalentity");
		Assert.assertEquals("aaa", data.get(0));
		Assert.assertEquals("bbb", data.get(1));
	}

	@Test
	public void testGet() {
		List<String> data = (List<String>) secondCache.get(query, db);
		Assert.assertEquals("ccc", data.get(0));
		Assert.assertEquals("ddd", data.get(1));
	}

}
