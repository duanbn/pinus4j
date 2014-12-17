package org.pinus.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus.api.IShardingKey;
import org.pinus.api.ShardingKey;
import org.pinus.api.enums.EnumDB;
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.cluster.DB;
import org.pinus.cluster.IDBCluster;
import org.pinus.cluster.beans.DBTable;
import org.pinus.cluster.impl.AppDBClusterImpl;
import org.pinus.entity.TestEntity;

public class DBClusterTest {

	public static final String CLUSTER_KLSTORAGE = "pinus";

	private IDBCluster dbCluster;

	public DBClusterTest() throws Exception {
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
		DB db = dbCluster.selectDbFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDbFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 1500800l);
		db = dbCluster.selectDbFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 1500811l);
		db = dbCluster.selectDbFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);
	}

}
