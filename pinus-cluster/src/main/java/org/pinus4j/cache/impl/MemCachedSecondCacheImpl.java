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

package org.pinus4j.cache.impl;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import net.spy.memcached.MemcachedClient;

import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.utils.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemCachedSecondCacheImpl extends AbstractMemCachedCache implements ISecondCache {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(MemCachedSecondCacheImpl.class);

	private static final Random r = new Random();

	/**
	 * 构造方法.
	 * 
	 * @param servers
	 *            ip:port,ip:port
	 */
	public MemCachedSecondCacheImpl(String s, int expire) {
		super(s, expire);
	}

	@Override
	public Collection<SocketAddress> getAvailableServers() {
		if (this.memClient == null) {
			return null;
		}
		return this.memClient.getAvailableServers();
	}

	@Override
	public void putGlobal(String whereSql, String clusterName, String tableName, List data) {
		try {
			String versionKey = _buildGlobalVersion(clusterName, tableName);
			int version = r.nextInt(10000);
			if (!_exists(versionKey)) {
				this.memClient.incr(versionKey, 0, version);
			} else {
				version = Integer.parseInt((String) this.memClient.get(versionKey));
			}

			String cacheKey = _buildGlobalCacheKey(whereSql, clusterName, tableName, version);
			this.memClient.set(cacheKey, expire, data);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}
	}

	@Override
	public List getGlobal(String whereSql, String clusterName, String tableName) {
		try {
			String versionKey = _buildGlobalVersion(clusterName, tableName);
			if (_exists(versionKey)) {
				int version = Integer.parseInt((String) this.memClient.get(versionKey));

				String cacheKey = _buildGlobalCacheKey(whereSql, clusterName, tableName, version);
				List data = (List) this.memClient.get(cacheKey);

				if (LOG.isDebugEnabled() && data != null) {
					LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
				}

				return data;
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}

		return null;
	}

	@Override
	public void removeGlobal(String clusterName, String tableName) {
		String versionKey = _buildGlobalVersion(clusterName, tableName);
		if (_exists(versionKey)) {
			this.memClient.incr(versionKey, 1);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - " + versionKey + " clean");
			}
		}
	}

	@Override
	public void put(String whereSql, ShardingDBResource db, List data) {
		try {
			String versionKey = _buildShardingVersion(db);
			int version = r.nextInt(10000);
			if (!_exists(versionKey)) {
				this.memClient.incr(versionKey, 0, version);
			} else {
				version = Integer.parseInt((String) this.memClient.get(versionKey));
			}

			String cacheKey = _buildShardingCacheKey(whereSql, db, version);
			this.memClient.set(cacheKey, expire, data);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}
	}

	@Override
	public List get(String whereSql, ShardingDBResource db) {
		try {
			String versionKey = _buildShardingVersion(db);

			if (_exists(versionKey)) {
				int version = Integer.parseInt((String) this.memClient.get(versionKey));

				String cacheKey = _buildShardingCacheKey(whereSql, db, version);
				List data = (List) this.memClient.get(cacheKey);

				if (LOG.isDebugEnabled() && data != null) {
					LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
				}

				return data;
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}

		return null;
	}

	@Override
	public void remove(ShardingDBResource db) {
		String versionKey = _buildShardingVersion(db);
		if (_exists(versionKey)) {
			this.memClient.incr(versionKey, 1);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - " + versionKey + " clean");
			}
		}
	}

	private boolean _exists(String key) {
		return this.memClient.get(key) != null;
	}

	private String _buildGlobalVersion(String clusterName, String tableName) {
		StringBuilder versionKey = new StringBuilder("sec.version.");
		versionKey.append(clusterName).append(".");
		versionKey.append(tableName);
		return versionKey.toString();
	}

	public String _buildShardingVersion(ShardingDBResource shardingDBResource) {
		StringBuilder versionKey = new StringBuilder("sec.version.");
		versionKey.append(shardingDBResource.getClusterName()).append(shardingDBResource.getDbName());
		versionKey.append(".");
		versionKey.append(shardingDBResource.getRegionCapacity());
		versionKey.append(".");
		versionKey.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
		return versionKey.toString();
	}

	/**
	 * global second cache key. sec.[clustername].[tablename].[version].hashCode
	 */
	private String _buildGlobalCacheKey(String whereSql, String clusterName, String tableName, int version) {
		StringBuilder cacheKey = new StringBuilder("sec.");
		cacheKey.append(clusterName).append(".");
		cacheKey.append(tableName).append(".");
		cacheKey.append(version).append(".");
		cacheKey.append(SecurityUtil.md5(whereSql));
		return cacheKey.toString();
	}

	/**
	 * sharding second cache key. sec.[clustername].[startend].[tablename +
	 * tableIndex].[version].hashCode
	 */
	private String _buildShardingCacheKey(String whereSql, ShardingDBResource shardingDBResource, int version) {
		StringBuilder cacheKey = new StringBuilder("sec.");
		cacheKey.append(shardingDBResource.getClusterName()).append(shardingDBResource.getDbName());
		cacheKey.append(".");
		cacheKey.append(shardingDBResource.getRegionCapacity());
		cacheKey.append(".");
		cacheKey.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
		cacheKey.append(".");
		cacheKey.append(version).append(".");
		cacheKey.append(SecurityUtil.md5(whereSql));
		return cacheKey.toString();
	}

	public MemcachedClient getMemClient() {
		return memClient;
	}

	public void setMemClient(MemcachedClient memClient) {
		this.memClient = memClient;
	}

}
