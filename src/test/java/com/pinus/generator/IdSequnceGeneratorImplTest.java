package com.pinus.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.code.yanf4j.util.ConcurrentHashSet;
import com.pinus.BaseTest;

public class IdSequnceGeneratorImplTest extends BaseTest {

	public static final Set<Integer> ids = new ConcurrentHashSet<Integer>();
	public static final Set<Long> longIds = new ConcurrentHashSet<Long>();

	@Test
	public void test() throws Exception {
		IIdGenerator idGenerator = client.getIdGenerator();
		long start = System.currentTimeMillis();
		Thread th = null;
		List<Thread> ts = new ArrayList<Thread>();
		for (int i = 0; i < 20; i++) {
			th = new Gen(idGenerator);
			th.start();
			ts.add(th);
		}

		for (Thread t : ts) {
			t.join();
		}

		System.out.println(ids.size() + " = " + ids);
		System.out.println(longIds.size() + " = " + longIds);
		System.out.println("const " + (System.currentTimeMillis() - start) + "ms");
	}

	private class Gen extends Thread {
		private IIdGenerator idGen;

		public Gen(IIdGenerator idGen) {
			this.idGen = idGen;
		}

		public void run() {
			for (int i = 0; i < 10; i++) {
				int id = idGen.genClusterUniqueIntId(client.getDbCluster(), CLUSTER_NAME, "test_entity");
				long lId = idGen.genClusterUniqueLongId(client.getDbCluster(), CLUSTER_NAME, "testglobalentity");
				ids.add(id);
				longIds.add(lId);
			}
		}
	}

}
