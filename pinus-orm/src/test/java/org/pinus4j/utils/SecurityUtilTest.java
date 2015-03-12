package org.pinus4j.utils;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.utils.SecurityUtil;

public class SecurityUtilTest {

	@Test
	public void testMd5() {
		String str = "hello";

		Assert.assertEquals("5d41402abc4b2a76b9719d911017c592", SecurityUtil.md5(str));
	}

}
