package org.pinus4j.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.impl.AppDBClusterImpl;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.generator.beans.DBTable;

public class DBClusterTest {

	public static final String CLUSTER_KLSTORAGE = "pinus";

	private IDBCluster dbCluster;

	public DBClusterTest() throws Exception {
		this.dbCluster = new AppDBClusterImpl(EnumDB.MYSQL);
		this.dbCluster.setScanPackage("org.pinus4j.entity");
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
		List<String> dbsStr = new ArrayList<String>(12);
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus1, tableName=test_entity, tableIndex=0, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus1]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus1, tableName=test_entity, tableIndex=1, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus1]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus1, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus1]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus2, tableName=test_entity, tableIndex=0, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus2]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus2, tableName=test_entity, tableIndex=1, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus2]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus2, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus2]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus5, tableName=test_entity, tableIndex=0, regionCapacity=30000001-60000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus5]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus5, tableName=test_entity, tableIndex=1, regionCapacity=30000001-60000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus5]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus5, tableName=test_entity, tableIndex=2, regionCapacity=30000001-60000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus5]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus6, tableName=test_entity, tableIndex=0, regionCapacity=30000001-60000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus6]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus6, tableName=test_entity, tableIndex=1, regionCapacity=30000001-60000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus6]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus6, tableName=test_entity, tableIndex=2, regionCapacity=30000001-60000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus6]");
		List<IDBResource> dbs = dbCluster.getAllMasterShardingDBResource(TestEntity.class);
		for (int i = 0; i < 12; i++) {
			Assert.assertEquals(dbsStr.get(i), dbs.get(i).toString());
		}
		Assert.assertEquals(12, dbs.size());
	}

	@Test
	public void testGetAllSlaveShardingDB() throws Exception {
		List<String> dbsStr = new ArrayList<String>(12);
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus3, tableName=test_entity, tableIndex=0, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus3]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus3, tableName=test_entity, tableIndex=1, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus3]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus3, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus3]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus4, tableName=test_entity, tableIndex=0, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus4]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus4, tableName=test_entity, tableIndex=1, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus4]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus4, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus4]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus7, tableName=test_entity, tableIndex=0, regionCapacity=30000001-60000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus7]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus7, tableName=test_entity, tableIndex=1, regionCapacity=30000001-60000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus7]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus7, tableName=test_entity, tableIndex=2, regionCapacity=30000001-60000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus7]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus8, tableName=test_entity, tableIndex=0, regionCapacity=30000001-60000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus8]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus8, tableName=test_entity, tableIndex=1, regionCapacity=30000001-60000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus8]");
		dbsStr.add("ShardingDBResource [clusterName=pinus, dbName=pinus8, tableName=test_entity, tableIndex=2, regionCapacity=30000001-60000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus8]");
		List<IDBResource> dbs = dbCluster.getAllSlaveShardingDBResource(TestEntity.class, EnumDBMasterSlave.SLAVE0);
		for (int i = 0; i < 12; i++) {
			Assert.assertEquals(dbsStr.get(i), dbs.get(i).toString());
		}
		Assert.assertEquals(12, dbs.size());
	}

	@Test
	public void testMasterSelect() throws Exception {
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		IDBResource db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus1, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus1]",
				db.toString());

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus2, tableName=test_entity, tableIndex=0, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus2]",
				db.toString());

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50080l);
		db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus1, tableName=test_entity, tableIndex=1, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus1]",
				db.toString());

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50081l);
		db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus2, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=MASTER, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus2]",
				db.toString());
	}

	@Test
	public void testSlaveSelect() throws Exception {
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		IDBResource db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus3, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus3]",
				db.toString());

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus4, tableName=test_entity, tableIndex=0, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus4]",
				db.toString());

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 1500800l);
		db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus3, tableName=test_entity, tableIndex=2, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus3]",
				db.toString());

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 1500811l);
		db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		Assert.assertEquals(
				"ShardingDBResource [clusterName=pinus, dbName=pinus4, tableName=test_entity, tableIndex=1, regionCapacity=1-30000000,60000001-90000000, masterSlave=SLAVE0, databaseProductName=MySQL, host=127.0.0.1:3306, catalog=pinus4]",
				db.toString());
	}

}
