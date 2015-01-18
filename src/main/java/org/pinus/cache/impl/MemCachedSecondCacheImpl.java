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

package org.pinus.cache.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import net.spy.memcached.MemcachedClient;

import org.pinus.api.query.IQuery;
import org.pinus.cache.ISecondCache;
import org.pinus.cluster.DB;
import org.pinus.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemCachedSecondCacheImpl implements ISecondCache {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(MemCachedSecondCacheImpl.class);

	/**
	 * Spy client
	 */
	private MemcachedClient memClient;

	/**
	 * 过期秒数.
	 * 
	 * 默认30秒
	 */
	private int expire = 30;

	private static final Random r = new Random();

	/**
	 * 默认构造方法.
	 */
	public MemCachedSecondCacheImpl() {
	}

	public MemCachedSecondCacheImpl(String s) {
		this(s, 0);
	}

	/**
	 * 构造方法.
	 * 
	 * @param servers
	 *            ip:port,ip:port
	 */
	public MemCachedSecondCacheImpl(String s, int expire) {
		try {
			List<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
			String[] addresses = s.split(",");
			InetSocketAddress socketAddress = null;
			for (String address : addresses) {
				String[] pair = address.split(":");
				socketAddress = new InetSocketAddress(pair[0], Integer.parseInt(pair[1]));
				servers.add(socketAddress);
			}
			this.memClient = new MemcachedClient(servers);
		} catch (Exception e) {
			throw new RuntimeException("连接memcached服务器失败", e);
		}

		if (expire > 0) {
			this.expire = expire;
		}
	}

	@Override
	public int getExpire() {
		return this.expire;
	}

	@Override
	public void destroy() {
		this.memClient.shutdown();
	}

	@Override
	public Collection<SocketAddress> getAvailableServers() {
		if (this.memClient == null) {
			return null;
		}
		return this.memClient.getAvailableServers();
	}

	@Override
	public void putGlobal(IQuery query, String clusterName, String tableName, List data) {
		try {
			String versionKey = _buildGlobalVersion(clusterName, tableName);
			int version = r.nextInt(10000);
			if (!_exists(versionKey)) {
				this.memClient.incr(versionKey, 0, version);
			} else {
				version = Integer.parseInt((String) this.memClient.get(versionKey));
			}

			String cacheKey = _buildGlobalCacheKey(query, clusterName, tableName, version);
			this.memClient.set(cacheKey, expire, data);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}
	}

	@Override
	public List getGlobal(IQuery query, String clusterName, String tableName) {
		try {
			String versionKey = _buildGlobalVersion(clusterName, tableName);
			if (_exists(versionKey)) {
				int version = Integer.parseInt((String) this.memClient.get(versionKey));

				String cacheKey = _buildGlobalCacheKey(query, clusterName, tableName, version);
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
	public void put(IQuery query, DB db, List data) {
		try {
			String versionKey = _buildShardingVersion(db);
			int version = r.nextInt(10000);
			if (!_exists(versionKey)) {
				this.memClient.incr(versionKey, 0, version);
			} else {
				version = Integer.parseInt((String) this.memClient.get(versionKey));
			}

			String cacheKey = _buildShardingCacheKey(query, db, version);
			this.memClient.set(cacheKey, expire, data);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}
	}

	@Override
	public List get(IQuery query, DB db) {
		try {
			String versionKey = _buildShardingVersion(db);

			if (_exists(versionKey)) {
				int version = Integer.parseInt((String) this.memClient.get(versionKey));

				String cacheKey = _buildShardingCacheKey(query, db, version);
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
	public void remove(DB db) {
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

	public String _buildShardingVersion(DB db) {
		StringBuilder versionKey = new StringBuilder("sec.version.");
		versionKey.append(db.getClusterName()).append(db.getDbIndex());
		versionKey.append(".");
		versionKey.append(db.getStart()).append(db.getEnd());
		versionKey.append(".");
		versionKey.append(db.getTableName()).append(db.getTableIndex());
		return versionKey.toString();
	}

	/**
	 * global second cache key. sec.[clustername].[tablename].[version].hashCode
	 */
	private String _buildGlobalCacheKey(IQuery query, String clusterName, String tableName, int version) {
		StringBuilder cacheKey = new StringBuilder("sec.");
		cacheKey.append(clusterName).append(".");
		cacheKey.append(tableName).append(".");
		cacheKey.append(version).append(".");
		cacheKey.append(SecurityUtil.md5(query.getWhereSql()));
		return cacheKey.toString();
	}

	/**
	 * sharding second cache key. sec.[clustername].[startend].[tablename +
	 * tableIndex].[version].hashCode
	 */
	private String _buildShardingCacheKey(IQuery query, DB db, int version) {
		StringBuilder cacheKey = new StringBuilder("sec.");
		cacheKey.append(db.getClusterName()).append(db.getDbIndex());
		cacheKey.append(".");
		cacheKey.append(db.getStart()).append(db.getEnd());
		cacheKey.append(".");
		cacheKey.append(db.getTableName()).append(db.getTableIndex());
		cacheKey.append(".");
		cacheKey.append(version).append(".");
		cacheKey.append(SecurityUtil.md5(query.getWhereSql()));
		return cacheKey.toString();
	}

	public MemcachedClient getMemClient() {
		return memClient;
	}

	public void setMemClient(MemcachedClient memClient) {
		this.memClient = memClient;
	}

}
