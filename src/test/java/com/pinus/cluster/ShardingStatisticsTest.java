package com.pinus.cluster;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.ShardingKey;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.beans.DBClusterStatInfo;
import com.pinus.entity.TestEntity;

public class ShardingStatisticsTest extends BaseTest {

	@Test
	public void testHash() throws Exception {
		long seed = 1;
		ShardingKey<String> sv = null;
		Map<DB, Integer> counter = new HashMap<DB, Integer>();
		for (int i = 0; i < 100000; i++) {
			sv = new ShardingKey<String>(CLUSTER_KLSTORAGE, String.valueOf(++seed));
			DB db = this.cacheClient.getDbCluster().selectDbFromMaster("test_entity", sv);
			int count = 1;
			if (counter.get(db) != null) {
				count += counter.get(db);
			}
			counter.put(db, count);
		}
		for (Map.Entry<DB, Integer> entry : counter.entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}

}
