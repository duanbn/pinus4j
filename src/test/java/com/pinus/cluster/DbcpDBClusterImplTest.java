package com.pinus.cluster;

import java.util.List;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.TestEntity;
import com.pinus.api.IShardingKey;
import com.pinus.api.ShardingKey;
import com.pinus.api.enums.EnumDBMasterSlave;

public class DbcpDBClusterImplTest extends BaseTest {

	@Test
	public void testGetAllMasterShardingDB() throws Exception {
		IDBCluster dbCluster = this.cacheClient.getDbCluster();
		List<DB> dbs = dbCluster.getAllMasterShardingDB(TestEntity.class);
		for (DB db : dbs) {
			System.out.println(db);
		}
		System.out.println(dbs.size());
	}

	@Test
	public void testGetAllSlaveShardingDB() throws Exception {
		IDBCluster dbCluster = this.cacheClient.getDbCluster();
		List<DB> dbs = dbCluster.getAllSlaveShardingDB(TestEntity.class, EnumDBMasterSlave.SLAVE0);
		for (DB db : dbs) {
			System.out.println(db);
		}
		System.out.println(dbs.size());
	}

	@Test
	public void testMasterSelect() throws Exception {
		IDBCluster dbCluster = this.cacheClient.getDbCluster();
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		DB db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50080l);
		db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50081l);
		db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);
	}

	@Test
	public void testSlaveSelect() throws Exception {
		IDBCluster dbCluster = this.cacheClient.getDbCluster();
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		DB db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50080l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50080l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);
	}

}
