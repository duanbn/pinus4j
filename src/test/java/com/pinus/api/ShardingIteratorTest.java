package com.pinus.api;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.TestEntity;
import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.datalayer.IShardingIterator;

public class ShardingIteratorTest extends BaseTest {

	@Test
	public void test() throws Exception {
        IQuery query = noCacheClient.createQuery();
        query.add(Condition.eq("testString", "testData"));
		IShardingIterator<TestEntity> it = noCacheClient.getShardingIterator(TestEntity.class, query);
		int i = 0;
		while (it.hasNext()) {
            it.next();
			//System.out.println(it.next());
			i++;
		}
		System.out.println("count " + i);
	}

}
