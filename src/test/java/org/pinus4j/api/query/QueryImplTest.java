package org.pinus4j.api.query;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.Order;
import org.pinus4j.api.query.QueryImpl;

public class QueryImplTest {

	@Test
	public void testClone() throws Exception {
		IQuery query = new QueryImpl();
		Condition cond = Condition.eq("field", "test field");
		query.add(cond).orderBy("field", Order.ASC).limit(0, 20);
		Assert.assertEquals(" WHERE field = 'test field' ORDER BY field asc LIMIT 0,20", query.getWhereSql());
		IQuery cloneOne = query.clone();
		Assert.assertEquals(" WHERE field = 'test field' ORDER BY field asc LIMIT 0,20", cloneOne.getWhereSql());
	}

	@Test
	public void testAdd() throws Exception {
		IQuery query = new QueryImpl();
		Condition cond = Condition.eq("field", "test field");
		query.add(cond);
		Assert.assertEquals(" WHERE field = 'test field'", query.getWhereSql());

		query.add(Condition.noteq("field", "test field to"));
		Assert.assertEquals(" WHERE field = 'test field' AND field <> 'test field to'", query.getWhereSql());
		query.add(Condition.in("field2", 2, 3, 4, 5));
		Assert.assertEquals(" WHERE field = 'test field' AND field <> 'test field to' AND field2 in (2,3,4,5)",
				query.getWhereSql());

		query.add(Condition.or(Condition.like("likeField", "like%"), Condition.in("infield", "5", "test")));
		Assert.assertEquals(
				" WHERE field = 'test field' AND field <> 'test field to' AND field2 in (2,3,4,5) AND (likeField like 'like%' OR infield in ('5','test'))",
				query.getWhereSql());

		query.orderBy("field", Order.DESC).limit(0, 10);
		Assert.assertEquals(
				" WHERE field = 'test field' AND field <> 'test field to' AND field2 in (2,3,4,5) AND (likeField like 'like%' OR infield in ('5','test')) ORDER BY field desc LIMIT 0,10",
				query.getWhereSql());
		
		IQuery zeroQuery = new QueryImpl();
		zeroQuery.add(Condition.eq("zero", 0));
		Assert.assertEquals(" WHERE zero = 0", zeroQuery.getWhereSql());
	}
}
