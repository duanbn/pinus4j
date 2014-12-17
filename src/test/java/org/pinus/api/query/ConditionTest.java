package org.pinus.api.query;

import org.junit.Test;
import org.pinus.api.query.Condition;

public class ConditionTest {

	@Test
	public void testEq() throws Exception {
		Condition cond = Condition.eq("field", "test");
		System.out.println(cond);

		cond = Condition.eq("field", 20);
		System.out.println(cond);
	}

	@Test
	public void testNoteq() throws Exception {
		Condition cond = Condition.noteq("field", "test");
		System.out.println(cond);

		cond = Condition.noteq("field", 20);
		System.out.println(cond);
	}

	@Test
	public void testGt() throws Exception {
		Condition cond = Condition.gt("field", 30);
		System.out.println(cond);
	}

	@Test
	public void testGte() throws Exception {
		Condition cond = Condition.gte("field", 30);
		System.out.println(cond);
	}

	@Test
	public void testLt() throws Exception {
		Condition cond = Condition.lt("field", 30);
		System.out.println(cond);
	}

	@Test
	public void testLte() throws Exception {
		Condition cond = Condition.lte("field", 30);
		System.out.println(cond);
	}

	@Test
	public void testIn() throws Exception {
		int[] is = new int[] { 1, 2, 3, 4 };
		Condition cond = Condition.in("field", is);
		System.out.println(cond);

		cond = Condition.in("field", new Integer[] { 2, 3, 4, 5 });
		System.out.println(cond);

		cond = Condition.in("field", "test", "test2");
		System.out.println(cond);

		cond = Condition.in("field", true, false);
		System.out.println(cond);
	}

	@Test
	public void testLike() throws Exception {
		Condition cond = Condition.like("field", "%field%");
		System.out.println(cond);
	}

	@Test
	public void testOr() throws Exception {
		Condition cond = Condition.or(Condition.eq("field", "testeq"), Condition.noteq("field2", 2));
		System.out.println(cond);
	}

}
