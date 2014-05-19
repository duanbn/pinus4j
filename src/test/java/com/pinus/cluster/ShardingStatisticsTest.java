package com.pinus.cluster;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.entity.TestEntity;
import com.pinus.BaseTest;
import com.pinus.api.ShardingValue;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.beans.DBClusterStatInfo;

public class ShardingStatisticsTest extends BaseTest {

	@Test
	public void testStatEntity() {
		IShardingStatistics shardingStatistics = this.client.getShardingStatistic();
		DBClusterStatInfo statInfo = shardingStatistics.statEntity(CLUSTER_NAME, TestEntity.class);
		for (Map.Entry<DB, Integer> entry : statInfo.getShardingEntityCount().entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}

	@Test
	public void testHash() throws Exception {
		long seed = 1000000000;
		ShardingValue<String> sv = null;
		Map<DB, Integer> counter = new HashMap<DB, Integer>();
		for (int i = 0; i < 100000; i++) {
			sv = new ShardingValue<String>(CLUSTER_NAME, String.valueOf(++seed));
			DB db = this.dbCluster.selectDbFromMaster("test_entity", sv);
			int count = 1;
			if (counter.get(db) != null) {
				count += counter.get(db);
			}
			counter.put(db, count);
			db.getDbConn().close();
		}
		for (Map.Entry<DB, Integer> entry : counter.entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}

}
