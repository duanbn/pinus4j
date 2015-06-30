package org.pinus4j.cache.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.SecurityUtil;
import org.pinus4j.utils.StringUtils;

import redis.clients.jedis.Jedis;

public class RedisSecondCacheImpl extends AbstractRedisCache implements ISecondCache {

    public RedisSecondCacheImpl(String address, int expire) {
        super(address, expire);
    }

    @Override
    public void putGlobal(String whereSql, String clusterName, String tableName, List data) {
        try {
            String cacheKey = _buildGlobalCacheKey(whereSql, clusterName, tableName);
            this.redisClient.set(cacheKey.getBytes(), IOUtil.getBytes(data));

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
            String cacheKey = _buildGlobalCacheKey(whereSql, clusterName, tableName);
            List data = IOUtil.getObject(this.redisClient.get(cacheKey.getBytes()), List.class);

            if (LOG.isDebugEnabled() && data != null) {
                LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("operate second cache failure");
        }

        return null;
    }

    @Override
    public void removeGlobal(String clusterName, String tableName) {
        try {
            List<String> keys = new ArrayList<String>();
            Collection<Jedis> shards = this.redisClient.getAllShards();
            String cacheKey = _buildGlobalCacheKey(null, clusterName, tableName);
            for (Jedis shard : shards) {
                keys.addAll(shard.keys(cacheKey));

                shard.del(keys.toArray(new String[0]));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - " + cacheKey + " clean");
            }
        } catch (Exception e) {
            LOG.warn("remove second cache failure");
        }
    }

    @Override
    public void put(String whereSql, ShardingDBResource db, List data) {
        try {
            String cacheKey = _buildShardingCacheKey(whereSql, db);
            this.redisClient.set(cacheKey.getBytes(), IOUtil.getBytes(data));

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
            String cacheKey = _buildShardingCacheKey(whereSql, db);
            List data = IOUtil.getObject(this.redisClient.get(cacheKey.getBytes()), List.class);

            if (LOG.isDebugEnabled() && data != null) {
                LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
            }

            return data;
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        }

        return null;
    }

    @Override
    public void remove(ShardingDBResource db) {
        try {
            List<String> keys = new ArrayList<String>();
            Collection<Jedis> shards = this.redisClient.getAllShards();
            String cacheKey = _buildShardingCacheKey(null, db);
            for (Jedis shard : shards) {
                keys.addAll(shard.keys(cacheKey));

                shard.del(keys.toArray(new String[0]));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - " + cacheKey + " clean");
            }
        } catch (Exception e) {
            LOG.warn("remove second cache failure");
        }
    }

    /**
     * global second cache key. sec.[clustername].[tablename].hashCode
     */
    private String _buildGlobalCacheKey(String whereSql, String clusterName, String tableName) {
        StringBuilder cacheKey = new StringBuilder("sec.");
        cacheKey.append(clusterName).append(".");
        cacheKey.append(tableName).append(".");
        if (StringUtils.isNotBlank(whereSql))
            cacheKey.append(SecurityUtil.md5(whereSql));
        else
            cacheKey.append("*");
        return cacheKey.toString();
    }

    /**
     * sharding second cache key. sec.[clustername].[startend].[tablename +
     * tableIndex].hashCode
     */
    private String _buildShardingCacheKey(String whereSql, ShardingDBResource shardingDBResource) {
        StringBuilder cacheKey = new StringBuilder("sec.");
        cacheKey.append(shardingDBResource.getClusterName());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getDbName());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getRegionCapacity());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
        cacheKey.append(".");
        if (StringUtils.isNotBlank(whereSql))
            cacheKey.append(SecurityUtil.md5(whereSql));
        else
            cacheKey.append("*");
        return cacheKey.toString();
    }

}
