package org.pinus.api.query;

import org.junit.Test;
import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.api.query.Order;
import org.pinus.api.query.QueryImpl;

public class QueryImplTest {

	@Test
	public void testClone() throws Exception {
		IQuery query = new QueryImpl();
		Condition cond = Condition.eq("field", "test field");
		query.add(cond).orderBy("field", Order.ASC).limit(0, 20);
		System.out.println(query);
		IQuery cloneOne = query.clone();
		System.out.println(cloneOne);
	}

	@Test
	public void testAdd() throws Exception {
		IQuery query = new QueryImpl();
		Condition cond = Condition.eq("field", "test field");
		query.add(cond);
		System.out.println(query);

		query.add(Condition.noteq("field", "test field to"));
		System.out.println(query);
		query.add(Condition.in("field2", 2, 3, 4, 5));
		System.out.println(query);

		query.add(Condition.or(Condition.like("likeField", "like%"), Condition.in("infield", "5", "test")));
		System.out.println(query);

		query.orderBy("field", Order.DESC).limit(0, 10);
		System.out.println(query);
	}
}
