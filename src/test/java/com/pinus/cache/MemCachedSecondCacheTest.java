package com.pinus.cache;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.IShardingKey;
import com.pinus.api.ShardingKey;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.cluster.DB;
import com.pinus.exception.DBClusterException;

public class MemCachedSecondCacheTest extends BaseTest {

	private String tableName = "test_entity";
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
			db = cacheClient.getDbCluster().selectDbFromMaster(tableName, shardingValue);
		} catch (DBClusterException e) {
			e.printStackTrace();
		}

		List<String> data = new ArrayList<String>();
		data.add("aaa");
		data.add("bbb");

		secondCache.putGlobal(query, CLUSTER_KLSTORAGE, tableName, data);

		data = new ArrayList<String>();
		data.add("ccc");
		data.add("ddd");
		secondCache.put(query, db, data);
	}

	@After
	public void after() {
		secondCache.removeGlobal(CLUSTER_KLSTORAGE, tableName);
		secondCache.remove(db);
	}

	@Test
	public void testGetGlobal() {
		List<String> data = (List<String>) secondCache.getGlobal(query, CLUSTER_KLSTORAGE, tableName);
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
