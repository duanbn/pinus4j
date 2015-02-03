package org.pinus.datalayer;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus.entity.TestEntity;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.QueryImpl;
import org.pinus4j.datalayer.SQLBuilder;

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
