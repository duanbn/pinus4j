package org.pinus4j;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.pinus4j.api.IShardingStorageClient;
import org.pinus4j.api.ShardingStorageClientImpl;
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.exceptions.LoadConfigException;

public class ApiBaseTest extends BaseTest {

	protected static IShardingStorageClient cacheClient = new ShardingStorageClientImpl();

	protected static IPrimaryCache primaryCache;

	protected static ISecondCache secondCache;

	@BeforeClass
	public static void startup() {
		cacheClient.setScanPackage("org.pinus4j");
		cacheClient.setSyncAction(EnumSyncAction.UPDATE);
		try {
			cacheClient.init();
		} catch (LoadConfigException e) {
			throw new RuntimeException(e);
		}

		primaryCache = cacheClient.getDBCluster().getPrimaryCache();

		secondCache = cacheClient.getDBCluster().getSecondCache();
	}

	@AfterClass
	public static void setdown() {
		cacheClient.destroy();
	}
	
	// @Test
	public void genData() throws Exception {
		List<TestEntity> dataList = new ArrayList<TestEntity>(3000);
		int i = 0;
		while (true) {
			dataList.add(createEntity());
			if (i++ % 3000 == 0) {
				Number[] pks = cacheClient.saveBatch(dataList,
						new ShardingKey<Integer>(CLUSTER_KLSTORAGE, r.nextInt(60000000)));
				System.out.println("save " + pks.length);
				dataList.clear();
			}
		}
	}

}
