package com.pinus.test.ser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import com.pinus.core.message.Message;
import com.pinus.core.ser.DeserializeException;
import com.pinus.core.ser.Deserializer;
import com.pinus.core.ser.MyDeserializer;
import com.pinus.core.ser.MySerializer;
import com.pinus.core.ser.SerializeException;
import com.pinus.core.ser.Serializer;
import com.pinus.test.BaseTest;
import com.pinus.test.Model;
import com.pinus.test.SubModel;

public class SerializerTest extends BaseTest {

	private Serializer ser = MySerializer.getInstance();
	private Deserializer deser = MyDeserializer.getInstance();

	@Test
	public void testThrowable() throws SerializeException, DeserializeException {
		RuntimeException e = new RuntimeException("test exception");
		byte[] b = ser.ser(e);
		RuntimeException de = (RuntimeException) deser.deser(b);
		System.out.println(de.getMessage());
	}

	@Test
	public void testClass() {
		try {
			byte[] b = ser.ser(Model.class);
			Class c = deser.deser(b, Class.class);
			System.out.println(c.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void pref() {
		try {
			SubModel sm = new SubModel();
			byte[] bb = null;
			for (int i = 0; i < 1000; i++) {
				bb = writeJson(sm);
				readJson(bb, SubModel.class);
				bb = ser.ser(sm, false);
				deser.deser(bb, false, Model.class);
				bb = writeObject(sm);
				readObject(bb);
				// bb = writeFjson(sm);
				// readFjson(bb, SubModel.class);
			}

			int times = 2000;
			System.out.println("ser and deser times=" + times);

			// jackson
			byte[] b = writeJson(sm);
			showLength("jackson length", b);
			long start = System.currentTimeMillis();
			for (int i = 0; i < times; i++)
				writeJson(sm);
			System.out.println("jackson ser use:" + (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			for (int i = 0; i < times; i++)
				readJson(b, SubModel.class);
			System.out.println("jackson deser use:" + (System.currentTimeMillis() - start));

			// jdk
			b = writeObject(sm);
			showLength("jdk length", b);
			start = System.currentTimeMillis();
			for (int i = 0; i < times; i++)
				writeObject(sm);
			System.out.println("jdk ser use:" + (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			for (int i = 0; i < times; i++)
				readObject(b);
			System.out.println("jdk deser use:" + (System.currentTimeMillis() - start));

			// fast json
			/*
			 * b = writeObject(sm); showLength("jdk length", b); start =
			 * System.currentTimeMillis(); for (int i=0; i<times; i++)
			 * writeFjson(sm); System.out.println("fastjson ser use:" +
			 * (System.currentTimeMillis() - start)); start =
			 * System.currentTimeMillis(); for (int i=0; i<times; i++)
			 * readFjson(b, SubModel.class);
			 * System.out.println("fastjson deser use:" +
			 * (System.currentTimeMillis() - start));
			 */

			// myrpc
			b = ser.ser(sm, false);
			showLength("myrpc length", b);
			start = System.currentTimeMillis();
			for (int i = 0; i < times; i++)
				ser.ser(sm, false);
			System.out.println("myrpc ser use:" + (System.currentTimeMillis() - start));
			start = System.currentTimeMillis();
			for (int i = 0; i < times; i++)
				deser.deser(b, false, Model.class);
			System.out.println("myrpc deser use:" + (System.currentTimeMillis() - start));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testModel() {
		try {
			Model sm = new Model();
			byte[] b = ser.ser(sm, false);
			showLength("myrpc write", b);
			Model sm1 = deser.deser(b, false, Model.class);

			b = writeObject(sm);
			showLength("java write", b);

			b = writeJson(sm);
			showLength("jackson write", b);

			b = writeFjson(sm);
			showLength("fast json write", b);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testList() {
		try {
			List<String> list = new ArrayList<String>();
			for (int i = 0; i < 10000; i++)
				list.add("aaa");

			byte[] b = ser.ser(list);
			showLength(b);

			b = writeObject(list);
			showLength(gzip(b));

			b = writeJson(list);
			showLength(gzip(b));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSet() {
		try {
			Set<String> set = new HashSet<String>();
			for (int i = 0; i < 10000; i++)
				set.add("aaa");

			byte[] b = ser.ser(set);
			Set s = deser.deser(b, Set.class);
			System.out.println(s);

			set = new TreeSet<String>();
			for (int i = 0; i < 10000; i++)
				set.add("aaa");

			b = ser.ser(set);
			s = deser.deser(b, Set.class);
			System.out.println(s);

			set = new LinkedHashSet<String>();
			for (int i = 0; i < 10000; i++)
				set.add("aaa");

			b = ser.ser(set);
			s = deser.deser(b, Set.class);
			System.out.println(s);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMap() {
		try {
			Map<String, SubModel> map = new HashMap<String, SubModel>();
			map.put("aa", new SubModel());
			map.put("bb", new SubModel());

			byte[] b = ser.ser(map);
			showLength(b);

			b = writeObject(map);
			showLength(b);

			b = writeJson(map);
			showLength(b);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testInt() {
		try {
			int i = 100;
			byte[] b = ser.ser(i);
			showLength(b);
			int di = deser.deser(b, Integer.TYPE);
			System.out.println(di);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testString() {
		try {
			String s = "中华人民共和国";
			byte[] b = ser.ser(s, false);
			showLength(b);

			b = writeObject(s);
			showLength(b);

			b = writeJson(s);
			showLength(b);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
