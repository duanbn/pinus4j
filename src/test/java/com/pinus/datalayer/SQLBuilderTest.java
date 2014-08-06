package com.pinus.datalayer;

import junit.framework.Assert;

import org.junit.Test;

import com.pinus.api.query.IQuery;
import com.pinus.api.query.QueryImpl;
import com.pinus.entity.TestEntity;

public class SQLBuilderTest {

	@Test
	public void testBuildSelectByQueryClassIntIQuery() {
		IQuery query = new QueryImpl();
		query.setFields("testInt", "testDouble");
		String sql = SQLBuilder.buildSelectByQuery(TestEntity.class, 0, query);
		Assert.assertEquals("SELECT testInt,testDouble FROM test_entity0", sql);

		query = new QueryImpl();
		sql = SQLBuilder.buildSelectByQuery(TestEntity.class, 0, query);
		Assert.assertEquals("SELECT * FROM test_entity0", sql);
	}
}
