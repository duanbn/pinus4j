package org.pinus;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus.api.IShardingStorageClient;
import org.pinus.api.ShardingKey;
import org.pinus.api.ShardingStorageClientImpl;
import org.pinus.api.enums.EnumMode;
import org.pinus.api.enums.EnumSyncAction;
import org.pinus.cache.IPrimaryCache;
import org.pinus.cache.ISecondCache;
import org.pinus.cache.impl.MemCachedPrimaryCacheImpl;
import org.pinus.cache.impl.MemCachedSecondCacheImpl;
import org.pinus.entity.TestEntity;
import org.pinus.entity.TestGlobalEntity;

public class BaseTest {

	protected Random r = new Random();

	public static final String CLUSTER_KLSTORAGE = "pinus";

	public static final String CACHE_HOST = "127.0.0.1:11211";

	protected static IShardingStorageClient cacheClient = new ShardingStorageClientImpl();

	protected static IPrimaryCache primaryCache;

	protected static ISecondCache secondCache;

	@BeforeClass
	public static void setup() throws Exception {
		primaryCache = new MemCachedPrimaryCacheImpl(CACHE_HOST, 0);
		secondCache = new MemCachedSecondCacheImpl(CACHE_HOST, 0);

		cacheClient.setMode(EnumMode.DISTRIBUTED);
		cacheClient.setScanPackage("org.pinus");
		cacheClient.setCreateTable(true);
		cacheClient.setSyncAction(EnumSyncAction.UPDATE);
		cacheClient.setPrimaryCache(primaryCache);
		cacheClient.setSecondCache(secondCache);
		cacheClient.init();
	}

	@AfterClass
	public static void setdown() {
		cacheClient.destroy();
		primaryCache.destroy();
		secondCache.destroy();
	}

	String[] seeds = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };

	public String getContent(int len) {
		StringBuilder content = new StringBuilder();
		for (int i = 0; i < len; i++) {
			content.append(seeds[r.nextInt(9)]);
		}
		return content.toString();
	}

	public TestEntity createEntity() {
		TestEntity testEntity = new TestEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setOTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) r.nextInt(255));
		testEntity.setOTestByte((byte) r.nextInt(255));
		testEntity.setTestChar('a');
		testEntity.setOTestChar('a');
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(r.nextDouble());
		testEntity.setOTestDouble(r.nextDouble());
		testEntity.setTestFloat(0.15f);
		testEntity.setOTestFloat(0.15f);
		testEntity.setTestInt(r.nextInt(60000000));
		testEntity.setOTestInt(r.nextInt(60000000));
		testEntity.setTestLong(r.nextLong());
		testEntity.setOTestLong(r.nextLong());
		testEntity.setTestShort((short) r.nextInt(30000));
		testEntity.setOTestShort((short) r.nextInt(30000));
		testEntity.setTestString(getContent(r.nextInt(100)));
		testEntity.setTestTime(new Timestamp(System.currentTimeMillis()));
		return testEntity;
	}

	public TestGlobalEntity createGlobalEntity() {
		TestGlobalEntity testEntity = new TestGlobalEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setoTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) r.nextInt(255));
		testEntity.setoTestByte((byte) r.nextInt(255));
		testEntity.setTestChar('b');
		testEntity.setoTestChar('b');
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(r.nextDouble());
		testEntity.setoTestDouble(r.nextDouble());
		testEntity.setTestFloat(0.15f);
		testEntity.setoTestFloat(0.15f);
		testEntity.setTestInt(r.nextInt(60000000));
		testEntity.setoTestInt(r.nextInt(60000000));
		testEntity.setTestLong(r.nextLong());
		testEntity.setoTestLong(r.nextLong());
		testEntity.setTestShort((short) r.nextInt(30000));
		testEntity.setoTestShort((short) r.nextInt(30000));
		testEntity.setTestString(getContent(r.nextInt(100)));
		return testEntity;
	}

//	@Test
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
