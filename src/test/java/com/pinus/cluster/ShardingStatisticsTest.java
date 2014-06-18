package com.pinus.cluster;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.TestEntity;
import com.pinus.api.ShardingKey;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.beans.DBClusterStatInfo;

public class ShardingStatisticsTest extends BaseTest {

	@Test
	public void testStatEntity() {
		IShardingStatistics shardingStatistics = this.cacheClient.getShardingStatistic();
		DBClusterStatInfo statInfo = shardingStatistics.statEntity(CLUSTER_KLSTORAGE, TestEntity.class);
		for (Map.Entry<DB, Integer> entry : statInfo.getShardingEntityCount().entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}

	@Test
	public void testHash() throws Exception {
		long seed = 1000000000;
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
