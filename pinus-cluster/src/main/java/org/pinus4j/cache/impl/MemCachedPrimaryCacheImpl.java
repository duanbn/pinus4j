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
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * memcached缓存实现. Pinus存储主缓存的实现. 缓存中的数据不设置过期时间，Pinus存储负责缓存与数据库之间的数据一致性.
 * 
 * @author duanbn
 */
public class MemCachedPrimaryCacheImpl extends AbstractMemCachedCache implements IPrimaryCache {

    /**
     * 日志.
     */
    public static final Logger LOG = LoggerFactory.getLogger(MemCachedPrimaryCacheImpl.class);

    /**
     * 构造方法.
     * 
     * @param servers ip:port,ip:port
     */
    public MemCachedPrimaryCacheImpl(String s, int expire) {
        super(s, expire);
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

        List<String> keys = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();
        for (Map.Entry<EntityPK, ? extends Object> entry : data.entrySet()) {
            keys.add(buildGlobalKey(clusterName, tableName, entry.getKey()));
            datas.add(entry.getValue());
        }
        _put(keys, datas);
    }

    @Override
    public <T> Map<EntityPK, T> getGlobal(String clusterName, String tableName, EntityPK[] ids) {
        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
            String key = buildGlobalKey(clusterName, tableName, id);
            keys.add(key);
        }

        Map<EntityPK, T> result = Maps.newLinkedHashMap();

        Map<String, Object> data = _get(keys);
        if (data != null) {
            T value = null;
            for (int i = 0; i < ids.length; i++) {
                value = (T) data.get(keys.get(i));
                if (value != null)
                    result.put(ids[i], value);
            }
        }

        return result;
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

        List<String> keys = new ArrayList<String>();
        List<Object> datas = new ArrayList<Object>();
        for (Map.Entry<EntityPK, ? extends Object> entry : data.entrySet()) {
            keys.add(buildKey(db, entry.getKey()));
            datas.add(entry.getValue());
        }
        _put(keys, datas);
    }

    @Override
    public <T> Map<EntityPK, T> get(ShardingDBResource db, EntityPK[] ids) {
        Map<EntityPK, T> result = Maps.newLinkedHashMap();

        List<String> keys = new ArrayList<String>();
        for (EntityPK id : ids) {
            keys.add(buildKey(db, id));
        }
        Map<String, Object> data = _get(keys);
        if (data != null) {
            T value = null;
            for (int i = 0; i < ids.length; i++) {
                value = (T) data.get(keys.get(i));
                if (value != null)
                    result.put(ids[i], value);
            }
        }

        return result;
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
        try {
            _removeCount(key);
            this.memClient.incr(key, 0, count);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - " + key + " set count=" + count);
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }
    }

    private void _removeCount(String key) {
        try {
            this.memClient.delete(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - delete " + key);
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
            if (memClient.get(key) != null) {
                long count = memClient.incr(key, delta);
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
            String count = (String) memClient.get(key);
            if (StringUtil.isNotBlank(count)) {
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

    private void _put(List<String> keys, List<? extends Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        try {
            for (int i = 0; i < keys.size(); i++) {
                memClient.set(keys.get(i), expire, data.get(i));
            }
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("[PRIMARY CACHE] - put (" + keys.size() + ") to cache " + keys);
        }
    }

    private Map<String, Object> _get(List<String> keys) {
        try {
            Map<String, Object> dataMap = memClient.getBulk(keys);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[PRIMARY CACHE] - get" + keys + " hits = " + dataMap.size());
            }

            return dataMap;
        } catch (Exception e) {
            LOG.warn("操作缓存失败:" + e.getMessage());
        }

        return null;
    }

    private void _remove(String key) {
        try {
            memClient.delete(key);
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
