package org.pinus.datalayer;

import java.util.List;

import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.datalayer.jdbc.FatDB;
import org.pinus.entity.TestEntity;

public class FatDBTest extends BaseTest {

	@Test
	public void testFindByQueryQuery() throws Exception {
		List<FatDB<TestEntity>> list = this.cacheClient.getAllFatDB(TestEntity.class, EnumDBMasterSlave.MASTER);
		for (FatDB<TestEntity> fatDb : list) {
			List<TestEntity> entities = fatDb.loadByQuery(this.cacheClient.createQuery(), true);
			System.out.println(entities.size());
		}
	}

}
