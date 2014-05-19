package com.pinus.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.IShardingValue;
import com.pinus.api.ShardingValue;
import com.pinus.cluster.enums.HashAlgoEnum;

public class DbcpDBClusterImplTest extends BaseTest {

	@Test
	public void testSelect() throws Exception {
		IDBCluster dbCluster = this.client.getDbCluster();
		IShardingValue<String> shardingValue = null;
		DB db = null;
		for (int i = 0; i < 10; i++) {
			shardingValue = new ShardingValue<String>(CLUSTER_NAME, UUID.randomUUID().toString());
			db = dbCluster.selectDbFromMaster("test_entity", shardingValue);
			System.out.println(db);
		}
		
		for (int i = 0; i < 10; i++) {
			shardingValue = new ShardingValue<String>("user", UUID.randomUUID().toString());
			db = dbCluster.selectDbFromMaster("kaola_device", shardingValue);
			System.out.println(db);
		}
	}

	public void testHash() throws Exception {
		IDBCluster dbCluster = this.client.getDbCluster();
		dbCluster.getDbRouter().setHashAlgo(HashAlgoEnum.JS);

		IShardingValue<String> shardingValue = null;
		DB db = null;
		Map<DB, Integer> statMap = new HashMap<DB, Integer>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			shardingValue = new ShardingValue<String>(CLUSTER_NAME, UUID.randomUUID().toString());
			db = dbCluster.selectDbFromMaster("test_entity", shardingValue);
			if (statMap.get(db) != null) {
				statMap.put(db, statMap.get(db) + 1);
			} else {
				statMap.put(db, 1);
			}
			db.getDbConn().close();
			if (i % 1000000 == 0) {
				System.out.println(statMap.size());
			}
		}

		System.out.println("hash algo " + this.dbCluster.getDbRouter().getHashAlgo() + ", const time "
				+ (System.currentTimeMillis() - start) + "ms");
		for (Map.Entry<DB, Integer> entry : statMap.entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}

}
