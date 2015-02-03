package org.pinus4j.utils;

import org.junit.Assert;
import org.junit.Test;
import org.pinus4j.utils.IOUtil;

public class IOUtilTest {

	@Test
	public void testSerObject() {
		String a = "test object";
		byte[] b = IOUtil.getBytes(a);
		String c = IOUtil.getObject(b, String.class);
		Assert.assertEquals(a, c);
	}

}
