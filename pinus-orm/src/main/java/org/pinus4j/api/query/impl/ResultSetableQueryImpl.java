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

import java.lang.reflect.Array;
import java.util.List;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.datalayer.query.IShardingQuery;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;

import com.google.common.collect.Lists;

/**
 * 可以获取结果集的查询条件
 * 
 * @author shanwei Jul 27, 2015 6:12:23 PM
 */
public class ResultSetableQueryImpl<T> extends DefaultQueryImpl<T> {

    private IGlobalQuery       globalQuery;

    private IShardingQuery     shardingQuery;

    private IShardingKey<?>    shardingKey;

    private boolean            useCache          = true;

    private EnumDBMasterSlave  masterSlave       = EnumDBMasterSlave.AUTO;

    private IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();

    /**
     * 将当前Query查询转换为PK查询，提高缓存的命中率.
     * 
     * @return
     */
    private List<EntityPK> coverToEntityPK() {
        List<EntityPK> entityPkList = Lists.newArrayList();

        // 判断查询条件是否和主键名数量一致
        PKName[] pkNames = entityMetaManager.getPkName(this.clazz);
        if (pkNames.length != this.condList.size()) {
            return null;
        }

        // 判断查询条件是否满足只是包含主键查询并且条件之间是or关系
        boolean isMatch = true;
        Condition cond = null;
        for (int i = 0; i < this.condList.size(); i++) {
            cond = this.condList.get(i);
            // 主键之间是or
            if (i > 0 && cond.getConditionRelation() != ConditionRelation.OR) {
                isMatch = false;
                break;
            }

            // 判断单主键是否满足条件
            else if (pkNames.length == 1 && (cond.getOrCond() == null || cond.getOrCond().length == 0)
                    && (cond.getAndCond() == null || cond.getAndCond().length == 0)
                    && cond.getField().equals(pkNames[0].getValue())) {

                if (cond.getOpt() == QueryOpt.EQ) {
                    PKValue[] pkValues = new PKValue[] { PKValue.valueOf(cond.getValue()) };
                    entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
                }
                if (cond.getOpt() == QueryOpt.IN) {
                    Object obj = null;
                    PKValue[] pkValues = null;
                    for (int j = 0; j < Array.getLength(cond.getValue()); j++) {
                        obj = Array.get(cond.getValue(), j);
                        pkValues = new PKValue[] { PKValue.valueOf(obj) };
                        entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
                    }
                }
            }

            // 判断联合主键是否满足条件
            else if (pkNames.length > 1 && (cond.getOrCond() == null || cond.getOrCond().length == 0)
                    && cond.getAndCond() != null && cond.getAndCond().length == pkNames.length && cond.isAndCondAllEQ()) {
                PKValue[] pkValues = new PKValue[cond.getAndCond().length];
                for (int j = 0; j < cond.getAndCond().length; j++) {
                    if (!pkNames[j].getValue().equals(cond.getAndCond()[j].getField())) {
                        isMatch = false;
                        break;
                    }
                    pkValues[j] = PKValue.valueOf(cond.getAndCond()[j].getValue());
                }
                entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
            } else {
                isMatch = false;
            }
        }
        if (!isMatch) {
            return null;
        }

        return entityPkList;
    }

    public ResultSetableQueryImpl(Class<T> clazz) {
        super.clazz = clazz;
    }

    @Override
    public T load() {
        List<T> result = list();

        if (result.isEmpty()) {
            return null;
        }

        return result.get(0);
    }

    @Override
    public List<T> list() {
        List<T> result = null;

        List<EntityPK> entityPkList = coverToEntityPK();
        if (entityMetaManager.isShardingEntity(clazz)) {

            if (entityPkList != null && !entityPkList.isEmpty())
                if (this.shardingKey != null)
                    result = this.shardingQuery.findByPkList(entityPkList, this.shardingKey, this.clazz, this.useCache,
                            this.masterSlave);
                else
                    result = this.shardingQuery.findByPkList(entityPkList, this.clazz, this.useCache, this.masterSlave);
            else if (this.shardingKey != null)
                result = this.shardingQuery.findByQuery(this, this.shardingKey, this.clazz, this.useCache,
                        this.masterSlave);
            else
                result = this.shardingQuery.findByQuery(this, this.clazz, this.useCache, this.masterSlave);

        } else {

            if (entityPkList != null && !entityPkList.isEmpty())
                result = this.globalQuery.findByPkList(entityPkList, this.clazz, this.useCache, this.masterSlave);
            else
                result = this.globalQuery.findByQuery(this, this.clazz, this.useCache, this.masterSlave);

        }

        return result;
    }

    @Override
    public Number count() {
        Number count = 0;

        if (entityMetaManager.isShardingEntity(clazz)) {
            if (this.shardingKey != null) {
                if (this.isEffect()) {
                    count = this.shardingQuery.getCountByQuery(this, this.shardingKey, this.clazz, this.useCache,
                            this.masterSlave);
                } else {
                    count = this.shardingQuery.getCount(this.shardingKey, this.clazz, this.useCache, this.masterSlave);
                }
            } else {
                if (this.isEffect()) {
                    count = this.shardingQuery.getCountByQuery(this, this.clazz, this.useCache, this.masterSlave);
                } else {
                    count = this.shardingQuery.getCount(this.clazz, this.useCache, this.masterSlave);
                }
            }
        } else {
            if (this.isEffect()) {
                count = this.globalQuery.getCountByQuery(this, this.clazz, this.useCache, this.masterSlave);
            } else {
                count = this.globalQuery.getCount(this.clazz, this.useCache, this.masterSlave);
            }
        }

        return count;
    }

    @Override
    public IQuery<T> setShardingKey(IShardingKey<?> shardingKey) {
        this.shardingKey = shardingKey;

        return this;
    }

    @Override
    public IQuery<T> setMasterSlave(EnumDBMasterSlave masterSlave) {
        this.masterSlave = masterSlave;

        return this;
    }

    @Override
    public IQuery<T> setUseCache(boolean useCache) {
        this.useCache = useCache;

        return this;
    }

    @Override
    public IQuery<T> clone() {
        ResultSetableQueryImpl<T> clone = new ResultSetableQueryImpl<T>(this.clazz);

        clone.fields = this.fields;
        clone.condList.addAll(this.condList);
        clone.orderList.addAll(this.orderList);
        clone.start = this.start;
        clone.limit = this.limit;

        clone.globalQuery = this.globalQuery;
        clone.shardingQuery = this.shardingQuery;
        clone.shardingKey = this.shardingKey;
        clone.useCache = this.useCache;
        clone.masterSlave = this.masterSlave;
        clone.entityMetaManager = this.entityMetaManager;

        return clone;
    }

    @Override
    public void clean() {
        super.clean();

        this.shardingKey = null;
        this.useCache = true;
        this.masterSlave = EnumDBMasterSlave.AUTO;
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
