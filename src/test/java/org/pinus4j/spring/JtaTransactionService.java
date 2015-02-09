package org.pinus4j.spring;

import org.pinus4j.api.IShardingStorageClient;
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
	public void saveData() {
		TestGlobalEntity testGlobalEntity = new TestGlobalEntity();
		testGlobalEntity.setId(10);
		TestEntity testEntity = new TestEntity();
		testEntity.setId(10);
		testEntity.setTestInt(10);

		storageClient.globalSave(testGlobalEntity);
		storageClient.save(testEntity);
	}

	@Transactional
	public void saveDataWithException() {
		TestGlobalEntity testGlobalEntity = new TestGlobalEntity();
		testGlobalEntity.setId(10);
		TestEntity testEntity = new TestEntity();
		testEntity.setId(10);
		testEntity.setTestInt(10);

		storageClient.globalSave(testGlobalEntity);
		storageClient.save(testEntity);

		throw new RuntimeException();
	}

}
