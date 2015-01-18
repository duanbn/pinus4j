package org.pinus.util;

import org.junit.Test;

public class SecurityUtilTest {

    @Test
    public void testMd5() {
        String str = "hello";

        System.out.println(SecurityUtil.md5(str));
    }

}
