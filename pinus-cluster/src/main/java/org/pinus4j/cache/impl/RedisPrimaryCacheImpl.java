package org.pinus4j.cache.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.ReflectUtil;
import org.pinus4j.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisPrimaryCacheImpl extends AbstractRedisCache implements IPrimaryCache {

    public static final Logger LOG = LoggerFactory.getLogger(RedisPrimaryCacheImpl.class);

    public RedisPrimaryCacheImpl(String address, int expire) {
        super(address, expire);
    }

    @Override
    public void setCountGlobal(String clusterName, String tableName, long count) {
        String key = buildGlobalCountKey(clusterName, tableName);
        _setCount(key, count);
    }

    @Override
    public void removeCountGlobal(String clusterName, String tableName) {
        String key = buildGlobalCountKey(clusterName, tableName);
        _removeCount(key);
    }

    @Override
    public long decrCountGlobal(String clusterName, String tableName, int delta) {
        String key = buildGlobalCountKey(clusterName, tableName);
        return _decrCount(key, delta);
    }

    @Override
    public long incrCountGlobal(String clusterName, String tableName, int delta) {
        String key = buildGlobalCountKey(clusterName, tableName);
        return _incrCount(key, delta);
    }

    @Override
    public long getCountGlobal(String clusterName, String tableName) {
        String key = buildGlobalCountKey(clusterName, tableName);
        return _getCount(key);
    }

    @Override
    public void putGlobal(String clusterName, String tableName, Number id, Object data) {
        if (data == null) {
            return;
        }

        String key = buildGlobalKey(clusterName, tableName, id);
        _put(key, data);
    }

    @Override
    public void putGlobal(String clusterName, String tableName, List<? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        for (Object d : data) {
            Number id = ReflectUtil.getPkValue(d);
            keys.add(buildGlobalKey(clusterName, tableName, id));
        }
        _put(keys, data);
    }

    @Override
    public void putGlobal(String clusterName, String tableName, Map<Number, ? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();
        for (Map.Entry<Number, ? extends Object> entry : data.entrySet()) {
            keys.add(buildGlobalKey(clusterName, tableName, entry.getKey()));
            datas.add(entry.getValue());
        }
        _put(keys, datas);
    }

    @Override
    public <T> T getGlobal(String clusterName, String tableName, Number id) {
        String key = buildGlobalKey(clusterName, tableName, id);
        return _get(key);
    }

    @Override
    public List<Object> getGlobal(String clusterName, String tableName, Number[] ids) {
        List<String> keys = new ArrayList<String>();
        for (Number id : ids) {
            String key = buildGlobalKey(clusterName, tableName, id);
            keys.add(key);
        }
        return _get(keys);
    }

    @Override
    public void removeGlobal(String clusterName, String tableName, Number id) {
        String key = buildGlobalKey(clusterName, tableName, id);
        _remove(key);
    }

    @Override
    public void removeGlobal(String clusterName, String tableName, List<? extends Number> ids) {
        List<String> keys = new ArrayList<String>();
        for (Number id : ids) {
            keys.add(buildGlobalKey(clusterName, tableName, id));
        }
        _remove(keys);
    }

    @Override
    public void setCount(ShardingDBResource db, long count) {
        String key = buildCountKey(db);
        _setCount(key, count);
    }

    @Override
    public void removeCount(ShardingDBResource db) {
        String key = buildCountKey(db);
        _removeCount(key);
    }

    @Override
    public long decrCount(ShardingDBResource db, long delta) {
        String key = buildCountKey(db);
        return _decrCount(key, delta);
    }

    @Override
    public long incrCount(ShardingDBResource db, long delta) {
        String key = buildCountKey(db);
        return _incrCount(key, delta);
    }

    @Override
    public long getCount(ShardingDBResource db) {
        String key = buildCountKey(db);
        return _getCount(key);
    }

    @Override
    public void put(ShardingDBResource db, Number id, Object data) {
        if (data == null) {
            return;
        }

        String key = buildKey(db, id);
        _put(key, data);
    }

    @Override
    public void put(ShardingDBResource db, Number[] ids, List<? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        for (Number id : ids) {
            keys.add(buildKey(db, id));
        }
        _put(keys, data);
    }

    @Override
    public void put(ShardingDBResource db, Map<Number, ? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();
        for (Map.Entry<Number, ? extends Object> entry : data.entrySet()) {
            keys.add(buildKey(db, entry.getKey()));
            datas.add(entry.getValue());
        }
        _put(keys, datas);
    }

    @Override
    public <T> T get(ShardingDBResource db, Number id) {
        String key = buildKey(db, id);
        return _get(key);
    }

    @Override
    public List<Object> get(ShardingDBResource db, Number... ids) {
        List<String> keys = new ArrayList<String>();
        for (Number id : ids) {
            keys.add(buildKey(db, id));
        }
        return _get(keys);
    }

    @Override
    public void remove(ShardingDBResource db, Number id) {
        String key = buildKey(db, id);
        _remove(key);
    }

    @Override
    public void remove(ShardingDBResource db, List<? extends Number> ids) {
        List<String> keys = new ArrayList<String>();
        for (Number id : ids) {
            keys.add(buildKey(db, id));
        }
        _remove(keys);
    }

    private void _setCount(String key, long count) {
        try {
            _removeCount(key);
            redisClient.incrBy(key, count);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - " + key + " set count=" + count);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }
    }

    private void _removeCount(String key) {
        try {
            redisClient.del(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - delete " + key);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }
    }

    private long _decrCount(String key, long delta) {
        try {
            if (redisClient.get(key) != null) {
                long count = redisClient.decrBy(key, delta);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[PRIMARY CACHE] - decr " + key + " " + delta);
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
            if (redisClient.get(key) != null) {
                long count = redisClient.incrBy(key, delta);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[PRIMARY CACHE] - incr " + key + " " + delta);
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
            String count = (String) redisClient.get(key);
            if (StringUtils.isNotBlank(count)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[PRIMARY CACHE] - get " + key + " " + count);
                }
                return Long.parseLong(count);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        return -1l;
    }

    private void _put(String key, Object data) {
        try {
            redisClient.set(key.getBytes(), IOUtil.getBytes(data));
            redisClient.expire(key, expire);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - put " + key + " value=" + data);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }
    }

    private void _put(List<String> keys, List<? extends Object> data) {
        try {
            for (int i = 0; i < keys.size(); i++) {
                _put(keys.get(i), data.get(i));
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("[PRIMARY CACHE] - put (" + keys.size() + ") to cache " + keys);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T _get(String key) {
        try {
            T obj = (T) IOUtil.getObject(redisClient.get(key.getBytes()), Object.class);
            if (LOG.isDebugEnabled()) {
                int hit = 0;
                if (obj != null) {
                    hit = 1;
                }
                LOG.debug("[PRIMARY CACHE] - get " + key + " hit=" + hit);
            }
            return obj;
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        return null;
    }

    private List<Object> _get(List<String> keys) {
        List<Object> datas = new ArrayList<Object>();
        try {
            for (String key : keys) {
                datas.add(_get(key));
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("[PRIMARY CACHE] - get" + keys + " hits = " + datas.size());
        }

        return datas;
    }

    private void _remove(String key) {
        try {
            redisClient.del(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - remove " + key);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }
    }

    private void _remove(List<String> keys) {
        try {
            for (String key : keys) {
                _remove(key);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }
    }

}
