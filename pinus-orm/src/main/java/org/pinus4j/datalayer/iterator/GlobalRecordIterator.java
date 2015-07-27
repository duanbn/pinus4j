package org.pinus4j.datalayer.iterator;

import java.sql.SQLException;
import java.util.List;

import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.Order;
import org.pinus4j.api.query.QueryImpl;
import org.pinus4j.cluster.resources.GlobalDBResource;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
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

    public GlobalRecordIterator(GlobalDBResource dbResource, Class<E> clazz) {
        super(clazz);

        this.dbResource = dbResource;

        this.maxId = getMaxId();
    }

    public long getMaxId() {
        long maxId = 0;

        IQuery query = new QueryImpl();
        query.limit(1).orderBy(pkName, Order.DESC,clazz);
        List<E> one;
        try {
            one = selectByQuery(this.dbResource, query, clazz);
        } catch (SQLException e1) {
            throw new DBOperationException(e1);
        }
        if (!one.isEmpty()) {
            E e = one.get(0);
            maxId = ReflectUtil.getNotUnionPkValue(e).getValueAsLong();
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
            IQuery query = this.query.clone();
            long high = this.latestId + step;
            query.add(Condition.gte(pkName, latestId, clazz)).add(Condition.lt(pkName, high, clazz));
            try {
                List<E> recrods = selectByQuery(this.dbResource, query, clazz);
                this.latestId = high;

                while (recrods.isEmpty() && this.latestId < maxId) {
                    query = this.query.clone();
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
