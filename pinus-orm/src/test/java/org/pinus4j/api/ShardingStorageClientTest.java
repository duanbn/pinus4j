package org.pinus4j.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class ShardingStorageClientTest extends BaseTest {

	private static TestGlobalEntity globalEntity;
	private static Number globalPk1;
	private static List<TestGlobalEntity> globalEntities;
	private static Number[] globalPks;
	private static IQuery globalQuery;

	private static TestEntity shardingEntity1;
	private static Number shardingPk1;
	private static IShardingKey<Number> shardingKey1;
	private static TestEntity shardingEntity2;
	private static Number shardingPk2;
	private static IShardingKey<Number> shardingKey2;
	private static List<TestEntity> shardingEntities;
	private static Number[] shardingPks;
	private static IShardingKey<Integer> manyKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 3);
	private static IQuery shardingQuery;

	private static IShardingStorageClient storageClient;

	/**
	 * create data.
	 */
	@BeforeClass
	public static void before() {
		storageClient = getStorageClient();

		// save and update global one
		globalEntity = createGlobalEntity();
		globalPk1 = storageClient.globalSave(globalEntity);
		globalEntity.setTestString("i am a global entity");
		storageClient.globalUpdate(globalEntity);

		// save and update more
		globalEntities = new ArrayList<TestGlobalEntity>();
		for (int i = 0; i < 5; i++) {
			globalEntities.add(createGlobalEntity());
		}
		globalPks = storageClient.globalSaveBatch(globalEntities, CLUSTER_KLSTORAGE);
		for (TestGlobalEntity one : globalEntities) {
			one.setTestString("i am a global entity batch");
		}
		storageClient.globalUpdateBatch(globalEntities, CLUSTER_KLSTORAGE);

		globalQuery = storageClient.createQuery();
		globalQuery.add(Condition.eq("testString", "i am a global entity batch"));

		// save and update one
		shardingEntity1 = createEntity();
		shardingEntity1.setTestInt(1);
		shardingPk1 = storageClient.save(shardingEntity1);
		shardingKey1 = new ShardingKey<Number>(CLUSTER_KLSTORAGE, shardingEntity1.getTestInt());
		shardingEntity1.setTestString("i am a sharding entity");
		storageClient.update(shardingEntity1);
		shardingEntity2 = createEntity();
		shardingEntity2.setTestInt(2);
		shardingPk2 = storageClient.save(shardingEntity2);
		shardingKey2 = new ShardingKey<Number>(CLUSTER_KLSTORAGE, shardingEntity2.getTestInt());
		shardingEntity2.setTestString("i am a sharding entity");
		storageClient.update(shardingEntity2);

		// save and update more
		shardingEntities = new ArrayList<TestEntity>(5);
		for (int i = 0; i < 5; i++) {
			shardingEntities.add(createEntity());
		}
		shardingPks = storageClient.saveBatch(shardingEntities, manyKey);
		for (TestEntity one : shardingEntities) {
			one.setTestString("i am a sharding entity batch");
		}
		storageClient.updateBatch(shardingEntities, manyKey);

		shardingQuery = storageClient.createQuery();
		shardingQuery.add(Condition.eq("testString", "i am a sharding entity batch"));
	}

	/**
	 * clean data.
	 */
	@AfterClass
	public static void after() {
		// remove one
		storageClient.globalRemoveByPk(globalPk1, TestGlobalEntity.class, CLUSTER_KLSTORAGE);
		// check remove one
		TestGlobalEntity globalEntity = storageClient.findByPk(globalPk1, TestGlobalEntity.class);
		Assert.assertNull(globalEntity);

		// remove more
		storageClient.globalRemoveByPks(CLUSTER_KLSTORAGE, TestGlobalEntity.class, globalPks);
		// check remove more
		List<TestGlobalEntity> globalEntities = storageClient.findByPkList(Arrays.asList(globalPks),
				TestGlobalEntity.class);
		Assert.assertEquals(0, globalEntities.size());

		// remove one
		storageClient.removeByPk(shardingPk1, shardingKey1, TestEntity.class);
		storageClient.removeByPk(shardingPk2, shardingKey2, TestEntity.class);
		// check remove one
		TestEntity shardingEntity = storageClient.findByPk(shardingPk1, shardingKey1, TestEntity.class);
		Assert.assertNull(shardingEntity);

		// remove more
		storageClient.removeByPks(manyKey, TestEntity.class, shardingPks);
		// check remove more
		List<TestEntity> shardingEntities = storageClient.findByPkList(Arrays.asList(shardingPks), manyKey,
				TestEntity.class);
		Assert.assertEquals(0, shardingEntities.size());

		storageClient.destroy();
	}

	@Test
	public void testGetCountClassOfQ() {
		// global
		long globalCount = storageClient.getCount(TestGlobalEntity.class).longValue();
		Assert.assertEquals(6, globalCount);

		// sharding
		long shardingCount = storageClient.getCount(TestEntity.class).longValue();
		Assert.assertEquals(7, shardingCount);
	}

	@Test
	public void testGetCountClassOfQBoolean() {
		// global
		long globalCount = storageClient.getCount(TestGlobalEntity.class, false).longValue();
		Assert.assertEquals(6, globalCount);

		// sharding
		long shardingCount = storageClient.getCount(TestEntity.class, false).longValue();
		Assert.assertEquals(7, shardingCount);
	}

	@Test
	public void testGetCountClassOfQEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountClassOfQBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountIShardingKeyOfQClassOfQ() {
		long count = storageClient.getCount(shardingKey1, TestEntity.class).longValue();
		Assert.assertEquals(1, count);

		count = storageClient.getCount(shardingKey2, TestEntity.class).longValue();
		Assert.assertEquals(1, count);

		count = storageClient.getCount(manyKey, TestEntity.class).longValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountIShardingKeyOfQClassOfQBoolean() {
		long count = storageClient.getCount(shardingKey1, TestEntity.class, false).longValue();
		Assert.assertEquals(1, count);

		count = storageClient.getCount(shardingKey2, TestEntity.class, false).longValue();
		Assert.assertEquals(1, count);

		count = storageClient.getCount(manyKey, TestEntity.class, false).longValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountIShardingKeyOfQClassOfQEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountIShardingKeyOfQClassOfQBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountByQueryClassOfQIQuery() {
		// global
		long count = storageClient.getCountByQuery(TestGlobalEntity.class, globalQuery).longValue();
		Assert.assertEquals(5, count);

		// sharding
		count = storageClient.getCountByQuery(TestEntity.class, shardingQuery).longValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountByQueryClassOfQIQueryBoolean() {
		// global
		long count = storageClient.getCountByQuery(TestGlobalEntity.class, globalQuery, false).longValue();
		Assert.assertEquals(5, count);

		// sharding
		count = storageClient.getCountByQuery(TestEntity.class, shardingQuery, false).longValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountByQueryClassOfQIQueryEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountByQueryClassOfQIQueryBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountByQueryIQueryIShardingKeyOfQClassOfQ() {
		long count = storageClient.getCountByQuery(shardingQuery, shardingKey1, TestEntity.class).longValue();
		Assert.assertEquals(0, count);

		count = storageClient.getCountByQuery(shardingQuery, shardingKey2, TestEntity.class).longValue();
		Assert.assertEquals(0, count);

		count = storageClient.getCountByQuery(shardingQuery, manyKey, TestEntity.class).longValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountByQueryIQueryIShardingKeyOfQClassOfQBoolean() {
		long count = storageClient.getCountByQuery(shardingQuery, shardingKey1, TestEntity.class, false).longValue();
		Assert.assertEquals(0, count);

		count = storageClient.getCountByQuery(shardingQuery, shardingKey2, TestEntity.class, false).longValue();
		Assert.assertEquals(0, count);

		count = storageClient.getCountByQuery(shardingQuery, manyKey, TestEntity.class, false).longValue();
		Assert.assertEquals(5, count);
	}

	@Test
	public void testGetCountByQueryIQueryIShardingKeyOfQClassOfQEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testGetCountByQueryIQueryIShardingKeyOfQClassOfQBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkNumberClassOfT() {
		TestGlobalEntity globalEntity = storageClient.findByPk(ShardingStorageClientTest.globalPk1, TestGlobalEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.globalEntity, globalEntity);
		// test cache
		globalEntity = storageClient.findByPk(ShardingStorageClientTest.globalPk1, TestGlobalEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.globalEntity, globalEntity);

		TestEntity shardingEntity1 = storageClient.findByPk(ShardingStorageClientTest.shardingPk1, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);
		// test cache
		shardingEntity1 = storageClient.findByPk(ShardingStorageClientTest.shardingPk1, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);

		TestEntity shardingEntity2 = storageClient.findByPk(ShardingStorageClientTest.shardingPk2, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
		// test cache
		shardingEntity2 = storageClient.findByPk(ShardingStorageClientTest.shardingPk2, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
	}

	@Test
	public void testFindByPkNumberClassOfTBoolean() {
		TestGlobalEntity globalEntity = storageClient.findByPk(ShardingStorageClientTest.globalPk1, TestGlobalEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.globalEntity, globalEntity);
		// test cache
		globalEntity = storageClient.findByPk(ShardingStorageClientTest.globalPk1, TestGlobalEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.globalEntity, globalEntity);

		TestEntity shardingEntity1 = storageClient.findByPk(ShardingStorageClientTest.shardingPk1, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);
		// test cache
		shardingEntity1 = storageClient.findByPk(ShardingStorageClientTest.shardingPk1, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);

		TestEntity shardingEntity2 = storageClient.findByPk(ShardingStorageClientTest.shardingPk2, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
		// test cache
		shardingEntity2 = storageClient.findByPk(ShardingStorageClientTest.shardingPk2, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
	}

	@Test
	public void testFindByPkNumberClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkNumberClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkListListOfQextendsNumberClassOfT() {
		List<TestGlobalEntity> globalEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.globalPks),
				TestGlobalEntity.class);
		for (int i = 0; i < globalEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i), globalEntities.get(i));
		}
		// test cache
		globalEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.globalPks), TestGlobalEntity.class);
		for (int i = 0; i < globalEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i), globalEntities.get(i));
		}

		List<TestEntity> shardingEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.shardingPks),
				TestEntity.class);
		for (int i = 0; i < shardingEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
		// test cache
		shardingEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.shardingPks), TestEntity.class);
		for (int i = 0; i < shardingEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
	}

	@Test
	public void testFindByPkListListOfQextendsNumberClassOfTBoolean() {
		List<TestGlobalEntity> globalEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.globalPks),
				TestGlobalEntity.class, false);
		for (int i = 0; i < globalEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i), globalEntities.get(i));
		}

		List<TestEntity> shardingEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.shardingPks),
				TestEntity.class, false);
		for (int i = 0; i < shardingEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
	}

	@Test
	public void testFindByPkListListOfQextendsNumberClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkListListOfQextendsNumberClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindOneByQueryIQueryClassOfT() {
		TestGlobalEntity globalEntity = storageClient.findOneByQuery(globalQuery, TestGlobalEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(0), globalEntity);
		// test cache
		globalEntity = storageClient.findOneByQuery(globalQuery, TestGlobalEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(0), globalEntity);

		TestEntity shardingEntity = storageClient.findOneByQuery(shardingQuery, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(0), shardingEntity);
		// test cache
		shardingEntity = storageClient.findOneByQuery(shardingQuery, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(0), shardingEntity);
	}

	@Test
	public void testFindOneByQueryIQueryClassOfTBoolean() {
		TestGlobalEntity globalEntity = storageClient.findOneByQuery(globalQuery, TestGlobalEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(0), globalEntity);

		TestEntity shardingEntity = storageClient.findOneByQuery(shardingQuery, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(0), shardingEntity);
	}

	@Test
	public void testFindOneByQueryIQueryClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindOneByQueryIQueryClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByQueryIQueryClassOfT() {
		List<TestGlobalEntity> globalEntties = storageClient.findByQuery(globalQuery, TestGlobalEntity.class);
		for (int i = 0; i < globalEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i), globalEntties.get(i));
		}
		// test cache
		globalEntties = storageClient.findByQuery(globalQuery, TestGlobalEntity.class);
		for (int i = 0; i < globalEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i), globalEntties.get(i));
		}
		// FIXME: test page

		List<TestEntity> shardingEntities = storageClient.findByQuery(shardingQuery, TestEntity.class);
		for (int i = 0; i < globalEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
		// test cache
		shardingEntities = storageClient.findByQuery(shardingQuery, TestEntity.class);
		for (int i = 0; i < globalEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
		// FIXME: test page
	}

	@Test
	public void testFindByQueryIQueryClassOfTBoolean() {
		List<TestGlobalEntity> globalEntties = storageClient.findByQuery(globalQuery, TestGlobalEntity.class, false);
		for (int i = 0; i < globalEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i), globalEntties.get(i));
		}

		List<TestEntity> shardingEntities = storageClient.findByQuery(shardingQuery, TestEntity.class, false);
		for (int i = 0; i < globalEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
	}

	@Test
	public void testFindByQueryIQueryClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByQueryIQueryClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindBySqlSQLClassOfQ() {
		List<Map<String, Object>> globalEntities = storageClient.findBySql(
				SQL.valueOf("select * from testglobalentity where testString='i am a global entity batch'"),
				TestGlobalEntity.class);
		for (int i = 0; i < ShardingStorageClientTest.globalEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.globalEntities.get(i).getId(), globalEntities.get(i).get("id"));
		}
	}

	@Test
	public void testFindBySqlSQLClassOfQEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkNumberIShardingKeyOfQClassOfT() {
		TestEntity shardingEntity1 = storageClient.findByPk(shardingPk1, shardingKey1, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);
		// test cache
		shardingEntity1 = storageClient.findByPk(shardingPk1, shardingKey1, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);

		TestEntity shardingEntity2 = storageClient.findByPk(shardingPk2, shardingKey2, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
		// test cache
		shardingEntity2 = storageClient.findByPk(shardingPk2, shardingKey2, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
	}

	@Test
	public void testFindByPkNumberIShardingKeyOfQClassOfTBoolean() {
		TestEntity shardingEntity1 = storageClient.findByPk(shardingPk1, shardingKey1, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity1, shardingEntity1);

		TestEntity shardingEntity2 = storageClient.findByPk(shardingPk2, shardingKey2, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntity2, shardingEntity2);
	}

	@Test
	public void testFindByPkNumberIShardingKeyOfQClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkNumberIShardingKeyOfQClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkListListOfQextendsNumberIShardingKeyOfQClassOfT() {
		List<TestEntity> shardingEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.shardingPks), shardingKey1,
				TestEntity.class);
		for (int i = 0; i < shardingEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
		// test cache
		shardingEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.shardingPks), shardingKey1, TestEntity.class);
		for (int i = 0; i < shardingEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
	}

	@Test
	public void testFindByPkListListOfQextendsNumberIShardingKeyOfQClassOfTBoolean() {
		List<TestEntity> shardingEntities = storageClient.findByPkList(Arrays.asList(ShardingStorageClientTest.shardingPks), shardingKey1,
				TestEntity.class, false);
		for (int i = 0; i < shardingEntities.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntities.get(i));
		}
	}

	@Test
	public void testFindByPkListListOfQextendsNumberIShardingKeyOfQClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByPkListListOfQextendsNumberIShardingKeyOfQClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindOneByQueryIQueryIShardingKeyOfQClassOfT() {
		TestEntity shardingEntity = storageClient.findOneByQuery(shardingQuery, manyKey, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(0), shardingEntity);
		// test cache
		shardingEntity = storageClient.findOneByQuery(shardingQuery, manyKey, TestEntity.class);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(0), shardingEntity);
	}

	@Test
	public void testFindOneByQueryIQueryIShardingKeyOfQClassOfTBoolean() {
		TestEntity shardingEntity = storageClient.findOneByQuery(shardingQuery, manyKey, TestEntity.class, false);
		Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(0), shardingEntity);
	}

	@Test
	public void testFindOneByQueryIQueryIShardingKeyOfQClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindOneByQueryIQueryIShardingKeyOfQClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByQueryIQueryIShardingKeyOfQClassOfT() {
		List<TestEntity> shardingEntties = storageClient.findByQuery(shardingQuery, manyKey, TestEntity.class);
		for (int i = 0; i < shardingEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntties.get(i));
		}
		// test cache
		shardingEntties = storageClient.findByQuery(shardingQuery, manyKey, TestEntity.class);
		for (int i = 0; i < shardingEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntties.get(i));
		}
	}

	@Test
	public void testFindByQueryIQueryIShardingKeyOfQClassOfTBoolean() {
		List<TestEntity> shardingEntties = storageClient.findByQuery(shardingQuery, manyKey, TestEntity.class, false);
		for (int i = 0; i < shardingEntties.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i), shardingEntties.get(i));
		}
	}

	@Test
	public void testFindByQueryIQueryIShardingKeyOfQClassOfTEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindByQueryIQueryIShardingKeyOfQClassOfTBooleanEnumDBMasterSlave() {
		// TODO
	}

	@Test
	public void testFindBySqlSQLIShardingKeyOfQ() {
		List<Map<String, Object>> result = storageClient.findBySql(
				SQL.valueOf("select * from test_entity where testString='i am a sharding entity batch'"), manyKey);
		for (int i = 0; i < result.size(); i++) {
			Assert.assertEquals(ShardingStorageClientTest.shardingEntities.get(i).getId(), result.get(i).get("id"));
		}
	}

	@Test
	public void testFindBySqlSQLIShardingKeyOfQEnumDBMasterSlave() {
		// TODO
	}

}
