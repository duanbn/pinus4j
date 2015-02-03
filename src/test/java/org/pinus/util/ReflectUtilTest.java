package org.pinus.util;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus.entity.TestEntity;
import org.pinus4j.utils.ReflectUtil;

public class ReflectUtilTest extends BaseTest {

	@Test
	public void testGetPropery() throws Exception {
		TestEntity entity = createEntity();
		System.out.println(ReflectUtil.getProperty(entity, "testString"));
	}

	@Test
	public void testSetProperty() throws Exception {
		TestEntity entity = new TestEntity();
		ReflectUtil.setProperty(entity, "testString", "test name");
		System.out.println(entity.getTestString());
	}

	@Test
	public void testDescribe() throws Exception {
		TestEntity entity = createEntity();
		Map<String, Object> map = ReflectUtil.describe(entity);
		map.remove("testByte");
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			System.out.println(entry);
		}

		map = ReflectUtil.describe(entity, true, false);
		map.remove("testByte");
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			System.out.println(entry);
		}
		
		map = ReflectUtil.describe(entity, true, true);
		map.remove("testByte");
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			System.out.println(entry);
		}
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

}
