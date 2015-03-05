package org.pinus4j.spring;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:applicationContext.xml" })
public class JtaTransactionTest {

	@Autowired
	private JtaTransactionService service;

	@Test
	public void testCommit() throws Exception {
		long globalId = 10;
		long shardingId = 10;

		service.saveData(globalId, shardingId);

		TestGlobalEntity testGlobalEntity = service.getGlobalById(globalId);
		TestEntity testEntity = service.getShardingById(shardingId);

		Assert.assertNotNull(testGlobalEntity);
		Assert.assertNotNull(testEntity);

		service.removeData(globalId, shardingId);
	}

	@Test
	public void testRollback() {
		long globalId = 11;
		long shardingId = 11;

		try {
			service.saveDataWithException(globalId, shardingId);
		} catch (Exception e) {

		}

		TestGlobalEntity testGlobalEntity = service.getGlobalById(globalId);
		TestEntity testEntity = service.getShardingById(shardingId);

		Assert.assertNull(testGlobalEntity);
		Assert.assertNull(testEntity);

		service.removeData(globalId, shardingId);
	}

}
