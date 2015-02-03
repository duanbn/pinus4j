package org.pinus4j.utils;

import org.junit.Test;
import org.pinus4j.utils.SecurityUtil;

public class SecurityUtilTest {

    @Test
    public void testMd5() {
        String str = "hello";

        System.out.println(SecurityUtil.md5(str));
    }

}
