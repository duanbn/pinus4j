package com.pinus.api;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.TestEntity;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.datalayer.IShardingIterator;
import com.pinus.datalayer.beans.DBClusterIteratorInfo;

public class ShardingIteratorTest extends BaseTest {

	@Test
	public void test() throws Exception {
		IQuery query = noCacheClient.createQuery();
		query.add(Condition.eq("testString", "testData"));
		DBClusterIteratorInfo itInfo = new DBClusterIteratorInfo(1, 23, 111223);

		IShardingIterator<TestEntity> it = noCacheClient.getShardingIterator(TestEntity.class);
		int i = 0;
        long start = System.currentTimeMillis();
		while (it.hasNext()) {
			it.next();

			//System.out.println(it.curIteratorInfo());

			i++;
		}
		System.out.println("count " + i + ", const " + (System.currentTimeMillis() - start));
	}

}
