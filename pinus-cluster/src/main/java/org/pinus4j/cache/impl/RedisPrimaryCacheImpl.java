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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;

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
    public void putGlobal(String clusterName, String tableName, EntityPK id, Object data) {
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
            EntityPK entityPk = entityMetaManager.getEntityPK(d);
            keys.add(buildGlobalKey(clusterName, tableName, entityPk));
        }
        _put(keys, data);
    }

    @Override
    public void putGlobal(String clusterName, String tableName, Map<EntityPK, ? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();
        for (Map.Entry<EntityPK, ? extends Object> entry : data.entrySet()) {
            keys.add(buildGlobalKey(clusterName, tableName, entry.getKey()));
            datas.add(entry.getValue());
        }
        _put(keys, datas);
    }

    @Override
    public <T> T getGlobal(String clusterName, String tableName, EntityPK id) {
        String key = buildGlobalKey(clusterName, tableName, id);
        return _get(key);
    }

    @Override
    public List<Object> getGlobal(String clusterName, String tableName, EntityPK[] ids) {
        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
            String key = buildGlobalKey(clusterName, tableName, id);
            keys.add(key);
        }
        return _get(keys);
    }

    @Override
    public void removeGlobal(String clusterName, String tableName, EntityPK id) {
        String key = buildGlobalKey(clusterName, tableName, id);
        _remove(key);
    }

    @Override
    public void removeGlobal(String clusterName, String tableName, List<EntityPK> ids) {
        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
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
    public void put(ShardingDBResource db, EntityPK id, Object data) {
        if (data == null) {
            return;
        }

        String key = buildKey(db, id);
        _put(key, data);
    }

    @Override
    public void put(ShardingDBResource db, EntityPK[] ids, List<? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
            keys.add(buildKey(db, id));
        }
        _put(keys, data);
    }

    @Override
    public void put(ShardingDBResource db, Map<EntityPK, ? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();
        for (Map.Entry<EntityPK, ? extends Object> entry : data.entrySet()) {
            keys.add(buildKey(db, entry.getKey()));
            datas.add(entry.getValue());
        }
        _put(keys, datas);
    }

    @Override
    public <T> T get(ShardingDBResource db, EntityPK id) {
        String key = buildKey(db, id);
        return _get(key);
    }

    @Override
    public List<Object> get(ShardingDBResource db, EntityPK... ids) {
        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
            keys.add(buildKey(db, id));
        }
        return _get(keys);
    }

    @Override
    public void remove(ShardingDBResource db, EntityPK id) {
        String key = buildKey(db, id);
        _remove(key);
    }

    @Override
    public void remove(ShardingDBResource db, List<EntityPK> ids) {
        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
            keys.add(buildKey(db, id));
        }
        _remove(keys);
    }

    private void _setCount(String key, long count) {

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            _removeCount(key);
            redisClient.incrBy(key, count);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - " + key + " set count=" + count);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    private void _removeCount(String key) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            redisClient.del(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - delete " + key);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    private long _decrCount(String key, long delta) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            if (redisClient.get(key) != null) {
                long count = redisClient.decrBy(key, delta);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[PRIMARY CACHE] - decr " + key + " " + delta);
                }
                return count;
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return -1;
    }

    private long _incrCount(String key, long delta) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            if (redisClient.get(key) != null) {
                long count = redisClient.incrBy(key, delta);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[PRIMARY CACHE] - incr " + key + " " + delta);
                }
                return count;
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return -1;
    }

    private long _getCount(String key) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            String count = (String) redisClient.get(key);
            if (StringUtil.isNotBlank(count)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[PRIMARY CACHE] - get " + key + " " + count);
                }
                return Long.parseLong(count);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return -1l;
    }

    private void _put(String key, Object data) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            redisClient.set(key.getBytes(), IOUtil.getBytes(data));
            redisClient.expire(key.getBytes(), expire);

        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
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
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();
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
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return null;
    }

    private List<Object> _get(List<String> keys) {
        List<Object> datas = new ArrayList<Object>();
        try {
            Object value = null;
            for (String key : keys) {
                value = _get(key);
                if (value != null)
                    datas.add(value);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        return datas;
    }

    private void _remove(String key) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            redisClient.del(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - remove " + key);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
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
