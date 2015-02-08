/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.cluster.resources;

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
