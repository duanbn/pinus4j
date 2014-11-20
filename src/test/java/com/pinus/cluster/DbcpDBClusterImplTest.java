package com.pinus.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.pinus.api.IShardingKey;
import com.pinus.api.ShardingKey;
import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.impl.AppDBClusterImpl;
import com.pinus.entity.TestEntity;

public class DbcpDBClusterImplTest {

	public static final String CLUSTER_KLSTORAGE = "pinus";

	private IDBCluster dbCluster;

	public DbcpDBClusterImplTest() throws Exception {
		this.dbCluster = new AppDBClusterImpl(EnumDB.MYSQL);
		this.dbCluster.setScanPackage("com.pinus.entity");
		this.dbCluster.startup();
	}

	@Test
	public void testGetDBTable() {
		List<DBTable> tablesFromZk = this.dbCluster.getDBTableFromZk();
		Map<String, DBTable> zkDbTableMap = new HashMap<String, DBTable>();
		for (DBTable dbTable : tablesFromZk) {
			zkDbTableMap.put(dbTable.getName(), dbTable);
		}
		List<DBTable> tablesFromJvm = this.dbCluster.getDBTableFromJvm();
		for (DBTable dbTable : tablesFromJvm) {
			Assert.assertEquals(zkDbTableMap.get(dbTable.getName()), dbTable);
		}
	}

	@Test
	public void testGetAllMasterShardingDB() throws Exception {
		List<DB> dbs = dbCluster.getAllMasterShardingDB(TestEntity.class);
		for (DB db : dbs) {
			System.out.println(db);
		}
		System.out.println(dbs.size());
	}

	@Test
	public void testGetAllSlaveShardingDB() throws Exception {
		List<DB> dbs = dbCluster.getAllSlaveShardingDB(TestEntity.class, EnumDBMasterSlave.SLAVE0);
		for (DB db : dbs) {
			System.out.println(db);
		}
		System.out.println(dbs.size());
	}

	@Test
	public void testMasterSelect() throws Exception {
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
	}

}
