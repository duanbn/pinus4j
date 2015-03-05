package org.pinus4j.api.query;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.api.SQL;

public class SQLTest {

	@Test
	public void test() {
		SQL sql = SQL.valueOf("select * from test_entity where id=?", 10);
		Assert.assertEquals("select * from test_entity where id=10", sql.toString());

		sql = SQL.valueOf("select * from test_entity where id=10", null);
		Assert.assertEquals("select * from test_entity where id=10", sql.toString());

		sql = SQL.valueOf("select * from test_entity where testString=?", "");
		Assert.assertEquals("select * from test_entity where testString=''", sql.toString());

		sql = SQL.valueOf("select * from test_entity where testString=? AND testString1=?", "", "");
		Assert.assertEquals("select * from test_entity where testString='' AND testString1=''", sql.toString());
	}

}
