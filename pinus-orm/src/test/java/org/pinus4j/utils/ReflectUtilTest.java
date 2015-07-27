package org.pinus4j.utils;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.utils.ReflectUtil;

public class ReflectUtilTest extends BaseTest {

    @Test
    public void testGetPropery() throws Exception {
        TestEntity entity = createEntity();
        Assert.assertEquals('a', ReflectUtil.getProperty(entity, "testChar"));
    }

    @Test
    public void testSetProperty() throws Exception {
        TestEntity entity = new TestEntity();
        ReflectUtil.setProperty(entity, "testString", "test name");
        Assert.assertEquals("test name", entity.getTestString());

        ReflectUtil.setProperty(entity, "oTestInt", 1);
        Assert.assertEquals(1, entity.getOTestInt().intValue());
    }

    @Test
    public void testDescribe() throws Exception {
        TestEntity entity = createEntity();
        entity.setTestString(null);
        entity.setTestInt(0);
        Map<String, Object> map = ReflectUtil.describe(entity);
        map.remove("testByte");
        Assert.assertNull(map.get("testByte"));
        Assert.assertTrue(map.containsKey("testString"));
        Assert.assertTrue(map.containsKey("testInt"));
    }

    @Test
    public void testCloneWithGivenFieldObjectString() throws Exception {
        TestEntity entity = createEntity();
        TestEntity clone = (TestEntity) ReflectUtil.cloneWithGivenField(entity, "testInt", "testDouble");
        Assert.assertEquals(entity.getTestInt(), clone.getTestInt());
        Assert.assertEquals(entity.getTestDouble(), clone.getTestDouble());
        Assert.assertEquals(0.0f, clone.getTestFloat());
        Assert.assertNotNull(entity.getTestString());
        Assert.assertNull(clone.getTestString());
    }

    @Test
    public void testCopyProperties() throws Exception {
        TestEntity source = createEntity();
        TestEntity target = new TestEntity();
        ReflectUtil.copyProperties(source, target);
        Assert.assertEquals(source, target);
    }

}
