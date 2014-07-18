package com.pinus.util;

import org.junit.Assert;
import org.junit.Test;

public class IOUtilTest {

	@Test
	public void testSerObject() {
		String a = "test object";
		byte[] b = IOUtil.getBytes(a);
		String c = IOUtil.getObject(b, String.class);
		Assert.assertEquals(a, c);
	}

}
