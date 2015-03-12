package org.pinus4j.api.query;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.api.query.Condition;

public class ConditionTest {

	@Test
	public void testEq() throws Exception {
		Condition cond = Condition.eq("field", "test");
		Assert.assertEquals("field = 'test'", cond.toString());

		cond = Condition.eq("field", 20);
		Assert.assertEquals("field = 20", cond.toString());
	}

	@Test
	public void testNoteq() throws Exception {
		Condition cond = Condition.noteq("field", "test");
		Assert.assertEquals("field <> 'test'", cond.toString());

		cond = Condition.noteq("field", 20);
		Assert.assertEquals("field <> 20", cond.toString());
	}

	@Test
	public void testGt() throws Exception {
		Condition cond = Condition.gt("field", 30);
		Assert.assertEquals("field > 30", cond.toString());
	}

	@Test
	public void testGte() throws Exception {
		Condition cond = Condition.gte("field", 30);
		Assert.assertEquals("field >= 30", cond.toString());
	}

	@Test
	public void testLt() throws Exception {
		Condition cond = Condition.lt("field", 30);
		Assert.assertEquals("field < 30", cond.toString());
	}

	@Test
	public void testLte() throws Exception {
		Condition cond = Condition.lte("field", 30);
		Assert.assertEquals("field <= 30", cond.toString());
	}

	@Test
	public void testIn() throws Exception {
		int[] is = new int[] { 1, 2, 3, 4 };
		Condition cond = Condition.in("field", is);
		Assert.assertEquals("field in (1,2,3,4)", cond.toString());

		cond = Condition.in("field", new Integer[] { 2, 3, 4, 5 });
		Assert.assertEquals("field in (2,3,4,5)", cond.toString());

		cond = Condition.in("field", "test", "test2");
		Assert.assertEquals("field in ('test','test2')", cond.toString());

		cond = Condition.in("field", true, false);
		Assert.assertEquals("field in ('1','0')", cond.toString());
	}

	@Test
	public void testLike() throws Exception {
		Condition cond = Condition.like("field", "%field%");
		Assert.assertEquals("field like '%field%'", cond.toString());
	}

	@Test
	public void testOr() throws Exception {
		Condition cond = Condition.or(Condition.eq("field", "testeq"), Condition.noteq("field2", 2));
		Assert.assertEquals("(field = 'testeq' OR field2 <> 2)", cond.toString());
	}

}
