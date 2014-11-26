package com.pinus.cache.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import com.pinus.api.query.IQuery;
import com.pinus.cache.ISecondCache;
import com.pinus.cluster.DB;

public class MemCachedSecondCacheImpl implements ISecondCache {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(MemCachedSecondCacheImpl.class);

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
	public void putGlobal(IQuery query, String clusterName, String tableName, List<? extends Object> data) {
		try {
			String cacheKey = _buildGlobalCacheKey(query, clusterName, tableName);
			this.memClient.set(cacheKey, expire, data);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - put to second cache done, key:" + cacheKey);
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}
	}

	@Override
	public List<? extends Object> getGlobal(IQuery query, String clusterName, String tableName) {
		try {
			String cacheKey = _buildGlobalCacheKey(query, clusterName, tableName);
			List<? extends Object> data = (List<? extends Object>) this.memClient.get(cacheKey);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - second cache key " + cacheKey + " hit");
			}

			return data;
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}

		return null;
	}

	@Override
	public void removeGlobal(String clusterName, String tableName) {
		// memcached实现不支持根据前缀删除
		// FIXME:此方法暂不实现，只能等待缓存过期
	}

	@Override
	public void put(IQuery query, DB db, List<? extends Object> data) {
		try {
			String cacheKey = _buildShardingCacheKey(query, db);
			this.memClient.set(cacheKey, expire, data);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - put to second cache done, key:" + cacheKey);
			}
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}
	}

	@Override
	public List<? extends Object> get(IQuery query, DB db) {
		try {
			String cacheKey = _buildShardingCacheKey(query, db);
			List<? extends Object> data = (List<? extends Object>) this.memClient.get(cacheKey);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[SECOND CACHE] - second cache key " + cacheKey + " hit");
			}

			return data;
		} catch (Exception e) {
			LOG.warn("operate second cache failure");
		}

		return null;
	}

	@Override
	public void remove(DB db) {
		// memcached实现不支持根据前缀删除
		// FIXME:此方法暂不实现，只能等待缓存过期
	}

	private String _buildGlobalCacheKey(IQuery query, String clusterName, String tableName) {
		StringBuilder cacheKey = new StringBuilder("sec.");
		cacheKey.append(clusterName).append(".");
		cacheKey.append(tableName).append(".");
		cacheKey.append(query.getWhereSql().hashCode());
		return cacheKey.toString();
	}

	private String _buildShardingCacheKey(IQuery query, DB db) {
		StringBuilder cacheKey = new StringBuilder("sec.");
		cacheKey.append(db.getClusterName()).append(db.getDbIndex());
		cacheKey.append(".");
		cacheKey.append(db.getStart()).append(db.getEnd());
		cacheKey.append(".");
		cacheKey.append(db.getTableName()).append(db.getTableIndex());
		cacheKey.append(".");
		cacheKey.append(query.getWhereSql().hashCode());
		return cacheKey.toString();
	}

	public MemcachedClient getMemClient() {
		return memClient;
	}

	public void setMemClient(MemcachedClient memClient) {
		this.memClient = memClient;
	}

}
