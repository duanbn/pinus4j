package com.pinus.api;

import org.junit.Test;

import com.pinus.BaseTest;
import com.pinus.entity.KaolaUserRedHeart;

public class NoGlobalShardingTest extends BaseTest {

	@Test
	public void testRedHeart() throws Exception {
		KaolaUserRedHeart rh = new KaolaUserRedHeart();
		rh.setUid("abc");
		rh.setUserId(1);
		long id = this.cacheClient.save(rh).longValue();

		IShardingKey<Integer> key = new ShardingKey<Integer>("redheart", 1);
		this.cacheClient.removeByPk(id, key, KaolaUserRedHeart.class);
	}

}
