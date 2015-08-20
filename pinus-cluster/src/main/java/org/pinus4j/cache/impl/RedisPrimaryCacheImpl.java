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

import java.util.List;
import java.util.Map;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;

import com.google.common.collect.Maps;

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
    public void putGlobal(String clusterName, String tableName, Map<EntityPK, ? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        String key = buildGlobalKey(clusterName, tableName, null);

        _put(key, data);
    }

    @Override
    public <T> Map<EntityPK, T> getGlobal(String clusterName, String tableName, EntityPK[] pks) {
        String key = buildGlobalKey(clusterName, tableName, null);

        return _get(key, pks);
    }

    @Override
    public void removeGlobal(String clusterName, String tableName, List<EntityPK> pks) {
        String key = buildGlobalKey(clusterName, tableName, null);

        _remove(key, pks);
    }

    @Override
    public void setCount(ShardingDBResource db, long count) {
        String key = buildCountKey(db);
        _setCount(key, count);
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
    public void put(ShardingDBResource db, Map<EntityPK, ? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        String key = buildKey(db, null);

        _put(key, data);
    }

    @Override
    public <T> Map<EntityPK, T> get(ShardingDBResource dbResource, EntityPK[] pks) {
        String key = buildKey(dbResource, null);

        return _get(key, pks);
    }

    @Override
    public void remove(ShardingDBResource db, List<EntityPK> pks) {
        String key = buildKey(db, null);

        _remove(key, pks);
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

    private <T> void _put(String key, Map<EntityPK, T> param) {
        if (param == null || param.isEmpty()) {
            return;
        }

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            Map<byte[], byte[]> data = Maps.newLinkedHashMap();
            for (Map.Entry<EntityPK, T> entry : param.entrySet()) {
                data.put(IOUtil.getBytesByJava(entry.getKey()), IOUtil.getBytes(entry.getValue()));
            }

            redisClient.hmset(key.getBytes(), data);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - put (" + data.size() + ") to cache " + key);
            }

        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    private <T> Map<EntityPK, T> _get(String key, EntityPK[] pks) {
        Map<EntityPK, T> datas = Maps.newLinkedHashMap();

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            byte[][] fields = new byte[pks.length][];
            for (int i = 0; i < pks.length; i++) {
                fields[i] = IOUtil.getBytesByJava(pks[i]);
            }

            List<byte[]> result = redisClient.hmget(key.getBytes(), fields);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - get " + key + " hits = " + result.size());
            }

            T value = null;
            for (int i = 0; i < pks.length; i++) {
                value = (T) IOUtil.getObject(result.get(i), Object.class);
                if (value != null)
                    datas.put(pks[i], value);
            }

        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return datas;
    }

    private void _remove(String key, List<EntityPK> pks) {
        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            byte[][] fields = new byte[pks.size()][];
            for (int i = 0; i < pks.size(); i++) {
                fields[i] = IOUtil.getBytesByJava(pks.get(i));
            }
            redisClient.hdel(key.getBytes(), fields);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - remove " + key + " " + pks);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }
}
