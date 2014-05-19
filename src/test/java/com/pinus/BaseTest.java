package com.pinus;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.junit.After;
import org.junit.Before;

import com.entity.TestEntity;
import com.entity.TestGlobalEntity;
import com.pinus.api.IShardingStorageClient;
import com.pinus.api.ShardingStorageClientImpl;
import com.pinus.api.enums.EnumMode;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cache.impl.MemCachedPrimaryCacheImpl;
import com.pinus.cluster.IDBCluster;
import com.pinus.util.ReflectUtil;

public class BaseTest {

	protected Random r = new Random();

	public static final String CLUSTER_NAME = "klstorage";

	protected IShardingStorageClient client;
	protected IDBCluster dbCluster;
	protected IPrimaryCache primaryCache;

	@Before
	public void setup() {
		MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses("localhost:11211"));
		MemcachedClient memcachedClient = null;
		try {
			memcachedClient = builder.build();
		} catch (IOException e) {
			e.printStackTrace();
		}
		primaryCache = new MemCachedPrimaryCacheImpl(memcachedClient);

		client = new ShardingStorageClientImpl();
		client.setMode(EnumMode.DISTRIBUTED);
		client.setScanPackage("com.kaola.entity");
		client.setCreateTable(true);
		client.setPrimaryCache(primaryCache);
		client.init();

		dbCluster = client.getDbCluster();
	}

	@After
	public void setDown() {
		this.client.destroy();
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
		String name = ReflectUtil.getTableName(TestEntity.class);
		long id = client.genClusterUniqueLongId(CLUSTER_NAME, name);
		return createEntity(id);
	}

	public TestEntity createEntity(long id) {
		TestEntity testEntity = new TestEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) r.nextInt(255));
		testEntity.setTestChar((char) r.nextInt(97));
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(r.nextDouble());
		testEntity.setTestFloat(r.nextFloat());
		testEntity.setTestInt(r.nextInt());
		if (id > 0)
			testEntity.setTestId(id);
		testEntity.setTestLong(r.nextLong());
		testEntity.setTestShort((short) r.nextInt(30000));
		testEntity.setTestString(getContent(r.nextInt(100)));
		testEntity.setTestTime(new Timestamp(System.currentTimeMillis()));
		return testEntity;
	}

	public TestGlobalEntity createGlobalEntity() {
		return createGlobalEntity(0l);
	}

	public TestGlobalEntity createGlobalEntity(long id) {
		TestGlobalEntity testEntity = new TestGlobalEntity();
		testEntity.setTestBool(r.nextBoolean());
		testEntity.setTestByte((byte) r.nextInt(255));
		testEntity.setTestChar((char) r.nextInt(97));
		testEntity.setTestDate(new Date());
		testEntity.setTestDouble(r.nextDouble());
		testEntity.setTestFloat(r.nextFloat());
		testEntity.setTestInt(r.nextInt());
		if (id > 0)
			testEntity.setTestId(id);
		testEntity.setTestLong(r.nextLong());
		testEntity.setTestShort((short) r.nextInt(30000));
		testEntity.setTestString(getContent(r.nextInt(100)));
		return testEntity;
	}
	
}
