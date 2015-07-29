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

package org.pinus4j.api.query.impl;

import java.util.List;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.datalayer.query.IShardingQuery;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;

/**
 * 可以获取结果集的查询条件
 * 
 * @author shanwei Jul 27, 2015 6:12:23 PM
 */
public class ResultSetableQueryImpl<T> extends DefaultQueryImpl {

    private Class<T>           clazz;

    private IGlobalQuery       globalQuery;

    private IShardingQuery     shardingQuery;

    private boolean            useCache          = true;

    private EnumDBMasterSlave  masterSlave       = EnumDBMasterSlave.MASTER;

    private IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();

    public ResultSetableQueryImpl(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public List<T> list() {
        List<T> result = null;

        if (entityMetaManager.isShardingEntity(clazz)) {
            result = this.shardingQuery.findByQuery(this, this.clazz, this.useCache, this.masterSlave);
        } else {
            result = this.globalQuery.findByQuery(this, this.clazz, this.useCache, this.masterSlave);
        }

        return result;
    }

    @Override
    public Number count() {
        Number count = 0;

        if (entityMetaManager.isShardingEntity(clazz)) {
            count = this.shardingQuery.getCountByQuery(this, this.clazz, this.useCache, this.masterSlave);
        } else {
            count = this.globalQuery.getCountByQuery(this, this.clazz, this.useCache, this.masterSlave);
        }

        return count;
    }

    @Override
    public IQuery setMasterSlave(EnumDBMasterSlave masterSlave) {
        this.masterSlave = masterSlave;

        return this;
    }

    @Override
    public IQuery setUseCache(boolean useCache) {
        this.useCache = useCache;

        return this;
    }

    public IGlobalQuery getGlobalQuery() {
        return globalQuery;
    }

    public void setGlobalQuery(IGlobalQuery globalQuery) {
        this.globalQuery = globalQuery;
    }

    public IShardingQuery getShardingQuery() {
        return shardingQuery;
    }

    public void setShardingQuery(IShardingQuery shardingQuery) {
        this.shardingQuery = shardingQuery;
    }

}
