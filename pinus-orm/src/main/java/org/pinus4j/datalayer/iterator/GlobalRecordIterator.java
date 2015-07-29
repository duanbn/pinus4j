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

package org.pinus4j.datalayer.iterator;

import java.sql.SQLException;
import java.util.List;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.api.query.impl.Order;
import org.pinus4j.cluster.resources.GlobalDBResource;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.exceptions.DBOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库记录便利器. 注意此对象是线程不安全的.
 * 
 * @author duanbn
 * @param <E>
 */
public class GlobalRecordIterator<E> extends AbstractRecordIterator<E> {

    public static final Logger LOG = LoggerFactory.getLogger(GlobalRecordIterator.class);

    private IDBResource        dbResource;

    private IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();
    
    public GlobalRecordIterator(GlobalDBResource dbResource, Class<E> clazz) {
        super(clazz);

        this.dbResource = dbResource;

        this.maxId = getMaxId();
    }

    public long getMaxId() {
        long maxId = 0;

        IQuery query = new DefaultQueryImpl();
        query.limit(1).orderBy(pkName, Order.DESC, clazz);
        List<E> one;
        try {
            one = selectByQuery(this.dbResource, query, clazz);
        } catch (SQLException e1) {
            throw new DBOperationException(e1);
        }
        if (!one.isEmpty()) {
            E e = one.get(0);
            maxId = entityMetaManager.getNotUnionPkValue(e).getValueAsLong();
        }

        LOG.info("clazz " + clazz + " maxId=" + maxId);

        return maxId;
    }

    @Override
    public long getCount() {
        try {
            return selectCountByQuery(query, dbResource, clazz).longValue();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if (this.recordQ.isEmpty()) {
            IQuery query = ((DefaultQueryImpl) this.query).clone();
            long high = this.latestId + step;
            query.add(Condition.gte(pkName, latestId, clazz)).add(Condition.lt(pkName, high, clazz));
            try {
                List<E> recrods = selectByQuery(this.dbResource, query, clazz);
                this.latestId = high;

                while (recrods.isEmpty() && this.latestId < maxId) {
                    query = ((DefaultQueryImpl) this.query).clone();
                    high = this.latestId + step;
                    query.add(Condition.gte(pkName, this.latestId, clazz)).add(Condition.lt(pkName, high, clazz));
                    recrods = selectByQuery(this.dbResource, query, clazz);
                    this.latestId = high;
                }
                this.recordQ.addAll(recrods);
            } catch (SQLException e) {
                throw new DBOperationException(e);
            }
        }

        return !this.recordQ.isEmpty();
    }

}
