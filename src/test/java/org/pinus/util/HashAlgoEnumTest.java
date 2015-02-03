package org.pinus.util;

import org.junit.Test;
import org.pinus4j.cluster.enums.HashAlgoEnum;

public class HashAlgoEnumTest {

	@Test
	public void testHashAlgo() throws Exception {
		String udid = "a54756cc4e733d81fb0b355ca5a1dc7c";
		for (int i = 0; i < 10; i++)
			System.out.println(HashAlgoEnum.BERNSTEIN.hash(udid));
	}

}
