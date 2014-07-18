package com.pinus;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.pinus.api.IShardingStorageClient;
import com.pinus.api.ShardingStorageClientImpl;
import com.pinus.api.enums.EnumMode;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cache.impl.MemCachedPrimaryCacheImpl;
import com.pinus.entity.TestEntity;
import com.pinus.entity.TestGlobalEntity;

public class BaseTest {

	protected Random r = new Random();

	public static final String CLUSTER_KLSTORAGE = "klstorage";

	protected static IShardingStorageClient cacheClient = new ShardingStorageClientImpl();

	protected static IPrimaryCache primaryCache;

    @BeforeClass
	public static void before() {
		MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("localhost:11211"));
		MemcachedClient memcachedClient = null;
		try {
			memcachedClient = builder.build();
		} catch (IOException e) {
			e.printStackTrace();
		}
		primaryCache = new MemCachedPrimaryCacheImpl(memcachedClient);
		cacheClient.setMode(EnumMode.DISTRIBUTED);
		cacheClient.setScanPackage("com.pinus");
		cacheClient.setCreateTable(true);
		cacheClient.setPrimaryCache(primaryCache);
		cacheClient.init();
	}

	@AfterClass
	public static void after() {
		cacheClient.destroy();
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
		testEntity.setTestByte((byte) r.nextInt(255));
		testEntity.setTestChar((char) r.nextInt(97));
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(r.nextDouble());
		testEntity.setTestFloat(r.nextFloat());
		testEntity.setTestInt(r.nextInt());
		testEntity.setTestLong(r.nextLong());
		testEntity.setTestShort((short) r.nextInt(30000));
		testEntity.setTestString(getContent(r.nextInt(100)));
		testEntity.setTestTime(new Timestamp(System.currentTimeMillis()));
		return testEntity;
	}

	public TestGlobalEntity createGlobalEntity() {
		TestGlobalEntity testEntity = new TestGlobalEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) r.nextInt(255));
		testEntity.setTestChar((char) r.nextInt(97));
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(r.nextDouble());
		testEntity.setTestFloat(r.nextFloat());
		testEntity.setTestInt(r.nextInt());
		testEntity.setTestLong(r.nextLong());
		testEntity.setTestShort((short) r.nextInt(30000));
		testEntity.setTestString(getContent(r.nextInt(100)));
		return testEntity;
	}

	// @Test
	public void genData() throws Exception {
		TestEntity entity = null;
		for (int i = 0; i < r.nextInt(9999); i++) {
			entity = createEntity();
			cacheClient.save(entity);
		}
	}

}
