package org.pinus4j.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.api.IShardingKey;
import org.pinus4j.api.ShardingKey;
import org.pinus4j.api.enums.EnumDB;
import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.IDBResource;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.impl.AppDBClusterImpl;
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
		List<IDBResource> dbs = dbCluster.getAllMasterShardingDBResource(TestEntity.class);
		for (IDBResource db : dbs) {
			System.out.println(db);
		}
		System.out.println(dbs.size());
	}

	@Test
	public void testGetAllSlaveShardingDB() throws Exception {
		List<IDBResource> dbs = dbCluster.getAllSlaveShardingDBResource(TestEntity.class, EnumDBMasterSlave.SLAVE0);
		for (IDBResource db : dbs) {
			System.out.println(db);
		}
		System.out.println(dbs.size());
	}

	@Test
	public void testMasterSelect() throws Exception {
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		IDBResource db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50080l);
		db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 50081l);
		db = dbCluster.selectDBResourceFromMaster("test_entity", sv);
		System.out.println(db);
	}

	@Test
	public void testSlaveSelect() throws Exception {
		IShardingKey<Long> sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150080l);
		IDBResource db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 150081l);
		db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 1500800l);
		db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);

		sv = new ShardingKey<Long>(CLUSTER_KLSTORAGE, 1500811l);
		db = dbCluster.selectDBResourceFromSlave("test_entity", sv, EnumDBMasterSlave.SLAVE0);
		System.out.println(db);
	}

}
