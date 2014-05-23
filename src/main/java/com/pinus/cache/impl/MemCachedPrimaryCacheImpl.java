package com.pinus.cache.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.rubyeye.xmemcached.MemcachedClient;

import org.apache.log4j.Logger;

import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.DB;
import com.pinus.util.ReflectUtil;

/**
 * memcached缓存实现. 考拉存储主缓存的实现. 缓存中的数据不设置过期时间，考拉存储负责缓存与数据库之间的数据一致性.
 * 
 * @author duanbn
 */
public class MemCachedPrimaryCacheImpl implements IPrimaryCache {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(MemCachedPrimaryCacheImpl.class);

	/**
	 * XMemcached客户端.
	 */
	private MemcachedClient memClient;

	/**
	 * 默认构造方法.
	 */
	public MemCachedPrimaryCacheImpl() {
	}

	/**
	 * 构造方法.
	 * 
	 * @param memClient
	 *            memcache客户端
	 */
	public MemCachedPrimaryCacheImpl(MemcachedClient memClient) {
		this.memClient = memClient;
	}

	@Override
	public void setCountGlobal(String clusterName, String tableName, long count) {
		String key = _buildGlobalCountKey(clusterName, tableName);
		_setCount(key, count);
	}

	@Override
	public void removeCountGlobal(String clusterName, String tableName) {
		String key = _buildGlobalCountKey(clusterName, tableName);
		_removeCount(key);
	}

	@Override
	public long decrCountGlobal(String clusterName, String tableName, int delta) {
		String key = _buildGlobalCountKey(clusterName, tableName);
		return _decrCount(key, delta);
	}

	@Override
	public long incrCountGlobal(String clusterName, String tableName, int delta) {
		String key = _buildGlobalCountKey(clusterName, tableName);
		return _incrCount(key, delta);
	}

	@Override
	public long getCountGlobal(String clusterName, String tableName) {
		String key = _buildGlobalCountKey(clusterName, tableName);
		return _getCount(key);
	}

	@Override
	public void putGlobal(String clusterName, String tableName, Number id, Object data) {
		String key = _buildGlobalKey(clusterName, tableName, id);
		_put(key, data);
	}

	@Override
	public void putGlobal(String clusterName, String tableName, List<? extends Object> data) {
		List<String> keys = new ArrayList<String>();
		for (Object d : data) {
			Number id = ReflectUtil.getPkValue(d);
			keys.add(_buildGlobalKey(clusterName, tableName, id));
		}
		_put(keys, data);
	}

	@Override
	public void putGlobal(String clusterName, String tableName, Map<Number, ? extends Object> data) {
		List<String> keys = new ArrayList<String>();
		List<Object> datas = new ArrayList<Object>();
		for (Map.Entry<Number, ? extends Object> entry : data.entrySet()) {
			keys.add(_buildGlobalKey(clusterName, tableName, entry.getKey()));
			datas.add(entry.getValue());
		}
		_put(keys, datas);
	}

	@Override
	public <T> T getGlobal(String clusterName, String tableName, Number id) {
		String key = _buildGlobalKey(clusterName, tableName, id);
		return _get(key);
	}

	@Override
	public <T> List<T> getGlobal(String clusterName, String tableName, Number... ids) {
		List<String> keys = new ArrayList<String>();
		for (Number id : ids) {
			String key = _buildGlobalKey(clusterName, tableName, id);
			keys.add(key);
		}
		return _get(keys);
	}

	@Override
	public void removeGlobal(String clusterName, String tableName, Number id) {
		String key = _buildGlobalKey(clusterName, tableName, id);
		_remove(key);
	}

	@Override
	public void removeGlobal(String clusterName, String tableName, Number... ids) {
		List<String> keys = new ArrayList<String>();
		for (Number id : ids) {
			keys.add(_buildGlobalKey(clusterName, tableName, id));
		}
		_remove(keys);
	}

	@Override
	public void setCount(DB db, long count) {
		String key = _buildCountKey(db);
		_setCount(key, count);
	}

	@Override
	public void removeCount(DB db) {
		String key = _buildCountKey(db);
		_removeCount(key);
	}

	@Override
	public long decrCount(DB db, long delta) {
		String key = _buildCountKey(db);
		return _decrCount(key, delta);
	}

	@Override
	public long incrCount(DB db, long delta) {
		String key = _buildCountKey(db);
		return _incrCount(key, delta);
	}

	@Override
	public long getCount(DB db) {
		String key = _buildCountKey(db);
		return _getCount(key);
	}

	@Override
	public void put(DB db, Number id, Object data) {
		String key = _buildKey(db, id);
		_put(key, data);
	}

	@Override
	public void put(DB db, Number[] ids, List<? extends Object> data) {
		List<String> keys = new ArrayList<String>();
		for (Number id : ids) {
			keys.add(_buildKey(db, id));
		}
		_put(keys, data);
	}

	@Override
	public void put(DB db, Map<Number, ? extends Object> data) {
		List<String> keys = new ArrayList<String>();
		List<Object> datas = new ArrayList<Object>();
		for (Map.Entry<Number, ? extends Object> entry : data.entrySet()) {
			keys.add(_buildKey(db, entry.getKey()));
			datas.add(entry.getValue());
		}
		_put(keys, datas);
	}

	@Override
	public <T> T get(DB db, Number id) {
		String key = _buildKey(db, id);
		return _get(key);
	}

	@Override
	public <T> List<T> get(DB db, Number... ids) {
		List<String> keys = new ArrayList<String>();
		for (Number id : ids) {
			keys.add(_buildKey(db, id));
		}
		return _get(keys);
	}

	@Override
	public void remove(DB db, Number id) {
		String key = _buildKey(db, id);
		_remove(key);
	}

	@Override
	public void remove(DB db, Number... ids) {
		List<String> keys = new ArrayList<String>();
		for (Number id : ids) {
			keys.add(_buildKey(db, id));
		}
		_remove(keys);
	}

	private void _setCount(String key, long count) {
		try {
			this.memClient.incr(key, 0, count);

			if (LOG.isDebugEnabled()) {
				LOG.debug("[CACHE] - " + key + " set count=" + count);
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}
	}

	private void _removeCount(String key) {
		try {
			this.memClient.delete(key);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[CACHE] - delete " + key);
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}
	}

	private long _decrCount(String key, long delta) {
		try {
			if (memClient.get(key) != null) {
				long count = memClient.decr(key, delta);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[CACHE] - decr " + key + " " + delta);
				}
				return count;
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}

		return -1;
	}

	private long _incrCount(String key, long delta) {
		try {
			if (memClient.get(key) != null) {
				long count = memClient.incr(key, delta);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[CACHE] - incr " + key + " " + delta);
				}
				return count;
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}

		return -1;
	}

	private long _getCount(String key) {
		try {
			String sCount = memClient.get(key);
			if (sCount != null) {
				long count = Long.parseLong(sCount);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[CACHE] - get " + key + " " + count);
				}
				return count;
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}

		return -1l;
	}

	private void _put(String key, Object data) {
		try {
			if (!memClient.set(key, 0, data)) {
				LOG.warn("操作缓存失败");
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("[CACHE] - put " + key + " value=" + data);
				}
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}
	}

	private void _put(List<String> keys, List<? extends Object> data) {
		try {
			for (int i = 0; i < keys.size(); i++) {
				memClient.setWithNoReply(keys.get(i), 0, data.get(i));
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("[CACHE] - put (" + keys.size() + ") to cache " + keys);
		}
	}

	private <T> T _get(String key) {
		try {
			T obj = memClient.get(key);
			if (LOG.isDebugEnabled()) {
				int hit = 0;
				if (obj != null) {
					hit = 1;
				}
				LOG.debug("[CACHE] - get " + key + " hit=" + hit);
			}
			return obj;
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}

		return null;
	}

	private <T> List<T> _get(List<String> keys) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[CACHE] - get " + keys);
		}

		List<T> datas = new ArrayList<T>();
		try {
			Map<String, T> dataMap = memClient.get(keys);
			if (dataMap != null) {
				T data = null;
				for (String key : keys) {
					data = dataMap.get(key);
					if (data != null)
						datas.add(data);
				}
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("[CACHE] - hits = " + datas.size());
		}

		return datas;
	}

	private void _remove(String key) {
		try {
			if (!memClient.delete(key)) {
				LOG.warn("操作缓存失败:");
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("[CACHE] - remove " + key);
				}
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}
	}

	private void _remove(List<String> keys) {
		try {
			for (String key : keys) {
				memClient.deleteWithNoReply(key);
				if (LOG.isDebugEnabled()) {
					LOG.debug("[CACHE] - remove " + key);
				}
			}
		} catch (Exception e) {
			LOG.warn("操作缓存失败:" + e.getMessage());
		}
	}

	/**
	 * 创建全局表count主键. [clusterName].[tableName].c
	 */
	private String _buildGlobalCountKey(String clusterName, String tableName) {
		StringBuilder key = new StringBuilder();
		key.append(clusterName).append(".").append(tableName).append(".c");
		return key.toString();
	}

	/**
	 * 创建count主键. [clusterName + dbIndex].[start + end].[tableName +
	 * tableIndex].c
	 */
	private String _buildCountKey(DB db) {
		StringBuilder key = new StringBuilder();
		key.append(db.getClusterName()).append(db.getDbIndex());
		key.append(".");
		key.append(db.getStart()).append(db.getEnd());
		key.append(".");
		key.append(db.getTableName()).append(db.getTableIndex());
		key.append(".c");
		return key.toString();
	}

	/**
	 * 创建全局表主键. [clusterName].[tableName].[id]
	 */
	private String _buildGlobalKey(String clusterName, String tableName, Number id) {
		StringBuilder key = new StringBuilder();
		key.append(clusterName).append(".").append(tableName).append(".");
		key.append(id);
		return key.toString();
	}

	/**
	 * 创建memcached主键. [clusterName + dbIndex].[start + end].[tableName +
	 * tableIndex].[id]
	 */
	private String _buildKey(DB db, Number id) {
		StringBuilder key = new StringBuilder();
		key.append(db.getClusterName()).append(db.getDbIndex());
		key.append(".");
		key.append(db.getStart()).append(db.getEnd());
		key.append(".");
		key.append(db.getTableName()).append(db.getTableIndex());
		key.append(".");
		key.append(id);
		return key.toString();
	}

	public MemcachedClient getMemClient() {
		return memClient;
	}

	public void setMemClient(MemcachedClient memClient) {
		this.memClient = memClient;
	}

}
