package org.pinus4j.cluster;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * cache database resource.
 * 
 * @author duanbn
 *
 */
public class DBResourceCache {

	/**
	 * global db resource cache container.
	 */
	private static final Map<GlobalDBResource.FindKey, GlobalDBResource> globalDBResourceCache = new WeakHashMap<GlobalDBResource.FindKey, GlobalDBResource>();

	/**
	 * sharding db resource cache container.
	 */
	private static final Map<ShardingDBResource.FindKey, ShardingDBResource> shardingDBResourceCache = new WeakHashMap<ShardingDBResource.FindKey, ShardingDBResource>();

	public static GlobalDBResource getGlobalDBResource(GlobalDBResource.FindKey findKey) {
		return globalDBResourceCache.get(findKey);
	}

	public static void putGlobalDBResource(GlobalDBResource.FindKey findKey, GlobalDBResource globalDBResource) {
		globalDBResourceCache.put(findKey, globalDBResource);
	}

	public static ShardingDBResource getShardingDBResource(ShardingDBResource.FindKey findKey) {
		return shardingDBResourceCache.get(findKey);
	}

	public static void putShardingDBResource(ShardingDBResource.FindKey findKey, ShardingDBResource shardingDBResource) {
		shardingDBResourceCache.put(findKey, shardingDBResource);
	}

	/**
	 * clean cache.
	 */
	public static void clear() {
		globalDBResourceCache.clear();
		shardingDBResourceCache.clear();
	}

}
