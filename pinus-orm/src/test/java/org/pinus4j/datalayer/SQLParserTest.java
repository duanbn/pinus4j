package org.pinus4j.datalayer;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.datalayer.SQLParser;

public class SQLParserTest {

	private static final String SQL1 = "SELECT * FROM abc, bcd WHERE abc.id = bcd.abcId and abc.name = ?";
	private static final String SQL2 = "SELECT * FROM abc inner join bcd on abc.id = bcd.abcId";

	@Test
	public void testParseTableNameString() {
		List<String> tableNames = SQLParser.parseTableName(SQL1);
		Assert.assertEquals("abc", tableNames.get(0));
		Assert.assertEquals("bcd", tableNames.get(1));

		tableNames = SQLParser.parseTableName(SQL2);
		Assert.assertEquals("abc", tableNames.get(0));
		Assert.assertEquals("bcd", tableNames.get(1));
	}

	@Test
	public void testAddTableIndexStringInt() {
		String sql = SQLParser.addTableIndex(SQL1, 1);
		Assert.assertEquals("SELECT * FROM abc1, bcd1 WHERE abc1.id = bcd1.abcId and abc1.name = ?", sql);
		sql = SQLParser.addTableIndex(SQL2, 1);
		Assert.assertEquals("SELECT * FROM abc1 inner join bcd1 on abc1.id = bcd1.abcId", sql);
	}

}
