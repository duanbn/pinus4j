package com.pinus.cluster;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.IShardingKey;
import com.pinus.api.ShardingKey;
import com.pinus.api.enums.EnumDBMasterSlave;

public class DbcpDBClusterImplTest extends BaseTest {

	@Test
	public void testMasterSelect() throws Exception {
		IDBCluster dbCluster = this.client.getDbCluster();
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_NAME, 150080l);
		DB db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 150081l);
		db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 50080l);
		db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 50081l);
		db = dbCluster.selectDbFromMaster("test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();
	}

	@Test
	public void testSlaveSelect() throws Exception {
		IDBCluster dbCluster = this.client.getDbCluster();
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_NAME, 150080l);
		DB db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 150081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 50080l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 50081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE0, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();
		
		sv = new ShardingKey<Long>(CLUSTER_NAME, 150080l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();
		
		sv = new ShardingKey<Long>(CLUSTER_NAME, 150081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 50080l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();

		sv = new ShardingKey<Long>(CLUSTER_NAME, 50081l);
		db = dbCluster.selectDbFromSlave(EnumDBMasterSlave.SLAVE1, "test_entity", sv);
		System.out.println(db);
		db.getDbConn().close();
	}

}
