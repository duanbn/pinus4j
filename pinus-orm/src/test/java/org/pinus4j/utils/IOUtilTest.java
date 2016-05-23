package org.pinus4j.utils;

import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class IOUtilTest extends BaseTest {

    public TestEntity       testEntity       = createEntity();
    public TestGlobalEntity testGlobalEntity = createGlobalEntity();

    private static byte[]   a;
    private static byte[]   b;
    private static byte[]   c;
    private static byte[]   d;

    @Test
    public void testGetBytes() throws Exception {
        long start = System.nanoTime();
        a = IOUtil.getBytesByJava(testEntity);
        System.out.println(a.length);
        System.out.println("const " + (System.nanoTime() - start));
        start = System.nanoTime();
        b = IOUtil.getBytesByJava(testGlobalEntity);
        System.out.println(b.length);
        System.out.println("const " + (System.nanoTime() - start));

        start = System.nanoTime();
        c = IOUtil.getBytes(testEntity);
        System.out.println(c.length);
        System.out.println("const " + (System.nanoTime() - start));

        start = System.nanoTime();
        d = IOUtil.getBytes(testGlobalEntity);
        System.out.println(d.length);
        System.out.println("const " + (System.nanoTime() - start));
    }

    @Test
    public void testGetObject() throws Exception {
        long start = System.nanoTime();
        System.out.println(IOUtil.getObjectByJava(a, TestEntity.class));
        System.out.println("const " + (System.nanoTime() - start));
        start = System.nanoTime();
        System.out.println(IOUtil.getObjectByJava(b, TestGlobalEntity.class));
        System.out.println("const " + (System.nanoTime() - start));

        start = System.nanoTime();
        System.out.println(IOUtil.getObject(c, TestEntity.class));
        System.out.println("const " + (System.nanoTime() - start));

        start = System.nanoTime();
        System.out.println(IOUtil.getObject(d, TestGlobalEntity.class));
        System.out.println("const " + (System.nanoTime() - start));
    }

}
