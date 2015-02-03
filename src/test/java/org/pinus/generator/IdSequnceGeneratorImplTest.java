package org.pinus.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.pinus.BaseTest;
import org.pinus4j.constant.Const;
import org.pinus4j.generator.IIdGenerator;

public class IdSequnceGeneratorImplTest extends BaseTest {

	public static final Set<Integer> ids = new HashSet<Integer>();
	public static final Set<Long> longIds = new HashSet<Long>();

	private static final String TABLE_NAME = "test_entity";

	@Test
	public void testBatchGen() throws Exception {
		IIdGenerator idGenerator = cacheClient.getIdGenerator();

		int[] intIds = idGenerator.genClusterUniqueIntIdBatch(Const.ZK_PRIMARYKEY + "/" + CLUSTER_KLSTORAGE,
				TABLE_NAME, 10);
		for (int i = 0; i < intIds.length; i++) {
			System.out.print(intIds[i] + " ");
		}
		System.out.println("");
		long[] longIds = idGenerator.genClusterUniqueLongIdBatch(Const.ZK_PRIMARYKEY + "/" + CLUSTER_KLSTORAGE,
				TABLE_NAME, 10);
		for (int i = 0; i < longIds.length; i++) {
			System.out.print(longIds[i] + " ");
		}
		System.out.println("");
	}

	@Test
	public void testGen() throws Exception {
		IIdGenerator idGenerator = cacheClient.getIdGenerator();

		for (int i = 0; i < 10; i++) {
			System.out.println(idGenerator.genClusterUniqueLongId(Const.ZK_PRIMARYKEY + "/" + CLUSTER_KLSTORAGE,
					"test_entity"));
		}
	}

	@Test
	public void testConcurrent() throws Exception {
		IIdGenerator idGenerator = cacheClient.getIdGenerator();
		long start = System.currentTimeMillis();
		Thread th = null;
		List<Thread> ts = new ArrayList<Thread>();
		for (int i = 0; i < 10; i++) {
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
				int id = idGen.genClusterUniqueIntId(Const.ZK_PRIMARYKEY + "/" + CLUSTER_KLSTORAGE, "test_entity");
				long lId = idGen.genClusterUniqueLongId(Const.ZK_PRIMARYKEY + "/" + CLUSTER_KLSTORAGE,
						"testglobalentity");
				ids.add(id);
				longIds.add(lId);
			}
		}
	}

}
