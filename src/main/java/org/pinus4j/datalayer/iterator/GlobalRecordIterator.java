package org.pinus4j.datalayer.iterator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.Order;
import org.pinus4j.api.query.QueryImpl;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库记录便利器. 注意此对象是线程不安全的.
 * 
 * @author duanbn
 *
 * @param <E>
 */
public class GlobalRecordIterator<E> extends AbstractRecordIterator<E> {

	public static final Logger LOG = LoggerFactory.getLogger(GlobalRecordIterator.class);

	private DBInfo dbConnInfo;
	
	public GlobalRecordIterator(DBInfo dbConnInfo, Class<E> clazz) {
		super(clazz);

		this.dbConnInfo = dbConnInfo;
		
		this.maxId = getMaxId();
	}

	public long getMaxId() {
		long maxId = 0;

		IQuery query = new QueryImpl();
		query.limit(1).orderBy(pkName, Order.DESC);
		List<E> one = null;
		try {
			one = selectGlobalByQuery(this.dbConnInfo.getDatasource().getConnection(), query, clazz);
		} catch (SQLException e1) {
			throw new DBOperationException("获取max id失败");
		}
		if (!one.isEmpty()) {
			E e = one.get(0);
			maxId = ReflectUtil.getPkValue(e).longValue();
		}

		LOG.info("clazz " + clazz + " maxId=" + maxId);

		return maxId;
	}

	@Override
	public long getCount() {
		return selectGlobalCount(query, dbConnInfo, this.dbConnInfo.getClusterName(), clazz).longValue();
	}

	@Override
	public boolean hasNext() {
		if (this.recordQ.isEmpty()) {
			IQuery query = this.query.clone();
			long high = this.latestId + step;
			query.add(Condition.gte(pkName, latestId)).add(Condition.lt(pkName, high));
			List<E> recrods;
			Connection conn = null;
			try {
				conn = this.dbConnInfo.getDatasource().getConnection();
				recrods = selectGlobalByQuery(conn, query, clazz);
			} catch (SQLException e) {
				throw new DBOperationException(e);
			} finally {
				SQLBuilder.close(conn);
			}
			this.latestId = high;

			while (recrods.isEmpty() && this.latestId < maxId) {
				query = this.query.clone();
				high = this.latestId + step;
				query.add(Condition.gte(pkName, this.latestId)).add(Condition.lt(pkName, high));
				try {
					conn = this.dbConnInfo.getDatasource().getConnection();
					recrods = selectGlobalByQuery(conn, query, clazz);
				} catch (SQLException e) {
					throw new DBOperationException(e);
				} finally {
					SQLBuilder.close(conn);
				}
				this.latestId = high;
			}
			this.recordQ.addAll(recrods);
		}

		return !this.recordQ.isEmpty();
	}

}
