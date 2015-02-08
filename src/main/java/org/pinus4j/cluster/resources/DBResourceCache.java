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
	private static final Map<IResourceId, IDBResource> globalDBResourceCache = new WeakHashMap<IResourceId, IDBResource>();

	/**
	 * sharding db resource cache container.
	 */
	private static final Map<IResourceId, IDBResource> shardingDBResourceCache = new WeakHashMap<IResourceId, IDBResource>();

	public static IDBResource getGlobalDBResource(IResourceId resId) {
		return globalDBResourceCache.get(resId);
	}

	public static void putGlobalDBResource(IResourceId resId, IDBResource globalDBResource) {
		globalDBResourceCache.put(resId, globalDBResource);
	}

	public static IDBResource getShardingDBResource(IResourceId resId) {
		return shardingDBResourceCache.get(resId);
	}

	public static void putShardingDBResource(IResourceId resId, IDBResource shardingDBResource) {
		shardingDBResourceCache.put(resId, shardingDBResource);
	}

	/**
	 * clean cache.
	 */
	public static void clear() {
		globalDBResourceCache.clear();
		shardingDBResourceCache.clear();
	}

}
