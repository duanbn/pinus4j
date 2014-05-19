package com.pinus.util;

import java.util.Map;

import org.junit.Test;

import com.entity.TestEntity;
import com.pinus.BaseTest;

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
	}

}
