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

import java.util.Map;

import org.pinus4j.cache.ICache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKValue;

/**
 * @author bingnan.dbn Jun 25, 2015 3:33:10 PM
 */
public abstract class AbstractCache implements ICache {

    protected String              address;

    protected int                 expire            = 30;

    protected Map<String, String> properties;

    protected IEntityMetaManager  entityMetaManager = DefaultEntityMetaManager.getInstance();

    public AbstractCache(String address, int expire) {
        this.address = address;

        if (expire > 0) {
            this.expire = expire;
        }
    }

    @Override
    public int getExpire() {
        return this.expire;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
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
    protected String buildGlobalKey(String clusterName, String tableName, EntityPK entityPk) {
        StringBuilder key = new StringBuilder();
        key.append(clusterName).append(".").append(tableName).append(".");
        StringBuilder pks = new StringBuilder();
        for (PKValue pkValue : entityPk.getPkValues()) {
            pks.append(pkValue.getValueAsString());
        }
        key.append(pks.toString());
        return key.toString();
    }

    /**
     * build sharding cache key [clusterName + dbIndex].[start + end].[tableName
     * + tableIndex].[id]
     */
    protected String buildKey(ShardingDBResource shardingDBResource, EntityPK entityPk) {
        StringBuilder key = new StringBuilder();
        key.append(shardingDBResource.getClusterName()).append(shardingDBResource.getDbName());
        key.append(".");
        key.append(shardingDBResource.getRegionCapacity());
        key.append(".");
        key.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
        key.append(".");
        StringBuilder pks = new StringBuilder();
        for (PKValue pkValue : entityPk.getPkValues()) {
            pks.append(pkValue.getValueAsString());
        }
        key.append(pks.toString());
        return key.toString();
    }

}
