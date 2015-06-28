package org.pinus4j.cache.impl;

import org.pinus4j.cache.ICache;
import org.pinus4j.cluster.resources.ShardingDBResource;

/**
 * @author bingnan.dbn Jun 25, 2015 3:33:10 PM
 */
public abstract class AbstractCache implements ICache {

    protected int expire = 30;

    public AbstractCache(String address, int expire) {
        if (expire > 0) {
            this.expire = expire;
        }
    }

    @Override
    public int getExpire() {
        return this.expire;
    }

    /**
     * build global count key [clusterName].[tableName].c
     */
    protected String buildGlobalCountKey(String clusterName, String tableName) {
        StringBuilder key = new StringBuilder();
        key.append(clusterName).append(".").append(tableName).append(".c");
        return key.toString();
    }

    /**
     * build sharding count key [clusterName + dbIndex].[start + end].[tableName
     * + tableIndex].c
     */
    protected String buildCountKey(ShardingDBResource shardingDBResource) {
        StringBuilder key = new StringBuilder();
        key.append(shardingDBResource.getClusterName()).append(shardingDBResource.getDbName());
        key.append(".");
        key.append(shardingDBResource.getRegionCapacity());
        key.append(".");
        key.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
        key.append(".c");
        return key.toString();
    }

    /**
     * build global cache key [clusterName].[tableName].[id]
     */
    protected String buildGlobalKey(String clusterName, String tableName, Number id) {
        StringBuilder key = new StringBuilder();
        key.append(clusterName).append(".").append(tableName).append(".");
        key.append(id);
        return key.toString();
    }

    /**
     * build sharding cache key [clusterName + dbIndex].[start + end].[tableName
     * + tableIndex].[id]
     */
    protected String buildKey(ShardingDBResource shardingDBResource, Number id) {
        StringBuilder key = new StringBuilder();
        key.append(shardingDBResource.getClusterName()).append(shardingDBResource.getDbName());
        key.append(".");
        key.append(shardingDBResource.getRegionCapacity());
        key.append(".");
        key.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
        key.append(".");
        key.append(id);
        return key.toString();
    }

}
