package com.pinus.api;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.TestGlobalEntity;

public class ShardingStorageTest extends BaseTest {

	@Test
	public void testGlobalSave() throws Exception {
		TestGlobalEntity entity = createGlobalEntity();
		long pk = client.globalSave(entity).longValue();
		System.out.println("new pk=" + pk);
	}

	@Test
	public void testGlobalSaveBatch() throws Exception {
		List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
		for (int i = 0; i < 4; i++) {
			entities.add(createGlobalEntity());
		}
		Number[] pks = client.globalSaveBatch(entities, CLUSTER_NAME);
		for (Number pk : pks) {
			System.out.println(pk);
		}
	}

	@Test
	public void testGlobalUpdate() throws Exception {
		TestGlobalEntity entity = createGlobalEntity();
		entity.setId(224);
		client.globalUpdate(entity);
	}

	@Test
	public void testGlobalUpdateBatch() throws Exception {
		List<TestGlobalEntity> list = new ArrayList<TestGlobalEntity>();
		TestGlobalEntity entity = createGlobalEntity();
		entity.setId(228);
		list.add(entity);
		entity = createGlobalEntity();
		entity.setId(242);
		list.add(entity);
		entity = createGlobalEntity();
		entity.setId(243);
		list.add(entity);
		client.globalUpdateBatch(list, CLUSTER_NAME);
	}

	@Test
	public void testGlobalCount() throws Exception {
		long count = client.getGlobalCount(CLUSTER_NAME, TestGlobalEntity.class).longValue();
		System.out.println(count);
	}

}