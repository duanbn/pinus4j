package com.pinus.datalayer;

import java.util.List;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.datalayer.jdbc.FatDB;
import com.pinus.entity.TestEntity;

public class FatDBTest extends BaseTest {

	@Test
	public void testFindByQueryQuery() throws Exception {
		List<FatDB<TestEntity>> list = this.cacheClient.getAllFatDB(TestEntity.class, EnumDBMasterSlave.MASTER);
		for (FatDB<TestEntity> fatDb : list) {
			List<TestEntity> entities = fatDb.loadByQuery(this.cacheClient.createQuery());
			System.out.println(entities.size());
		}
	}

}
