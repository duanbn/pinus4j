package org.pinus4j.spring;

import org.pinus4j.BaseTest;
import org.pinus4j.api.IShardingStorageClient;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JtaTransactionService {

	@Autowired
	private IShardingStorageClient storageClient;

	@Transactional
	public void saveData(long globalId, long shardingId) {
		TestGlobalEntity testGlobalEntity = new TestGlobalEntity();
		testGlobalEntity.setId(globalId);
		TestEntity testEntity = new TestEntity();
		testEntity.setId(shardingId);
		testEntity.setTestInt(10);

		storageClient.globalSave(testGlobalEntity);
		storageClient.save(testEntity);
	}

	@Transactional(readOnly = true)
	public TestGlobalEntity getGlobalById(long globalId) {
		return storageClient.findByPk(globalId, TestGlobalEntity.class);
	}

	@Transactional(readOnly = true)
	public TestEntity getShardingById(long shardingId) {
		ShardingKey<Integer> sk = new ShardingKey<Integer>(BaseTest.CLUSTER_KLSTORAGE, 10);
		return storageClient.findByPk(shardingId, sk, TestEntity.class);
	}

	@Transactional
	public void saveDataWithException(long globalId, long shardingId) {
		TestGlobalEntity testGlobalEntity = new TestGlobalEntity();
		testGlobalEntity.setId(globalId);
		TestEntity testEntity = new TestEntity();
		testEntity.setId(shardingId);
		testEntity.setTestInt(10);

		storageClient.globalSave(testGlobalEntity);
		storageClient.save(testEntity);

		throw new RuntimeException();
	}

	@Transactional
	public void removeData(long globalId, long shardingId) {
		storageClient.globalRemoveByPk(globalId, TestGlobalEntity.class, BaseTest.CLUSTER_KLSTORAGE);
		IShardingKey<Integer> sk = new ShardingKey<Integer>(BaseTest.CLUSTER_KLSTORAGE, 10);
		storageClient.removeByPk(shardingId, sk, TestEntity.class);
	}

}
