package org.pinus.datalayer.iterator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.api.query.Order;
import org.pinus.api.query.QueryImpl;
import org.pinus.cluster.beans.DBConnectionInfo;
import org.pinus.datalayer.IRecordReader;
import org.pinus.datalayer.SQLBuilder;
import org.pinus.datalayer.jdbc.AbstractJdbcQuery;
import org.pinus.exception.DBOperationException;
import org.pinus.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalRecordIterator<E> extends AbstractJdbcQuery implements IRecordReader<E> {

	public static final Logger LOG = LoggerFactory.getLogger(GlobalRecordIterator.class);

	private Class<E> clazz;

	private IQuery query;

	private DBConnectionInfo dbConnInfo;

	private String pkName;
	private Queue<E> recordQ;
	private static final int STEP = 5000;
	private long latestId = 0;
	private long maxId;

	public GlobalRecordIterator(DBConnectionInfo dbConnInfo, Class<E> clazz) {
		this.dbConnInfo = dbConnInfo;
		this.clazz = clazz;

		try {
			// check pk type
			pkName = ReflectUtil.getPkName(clazz);
			Class<?> type = clazz.getDeclaredField(pkName).getType();
			if (type != Long.TYPE && type != Integer.TYPE && type != Short.TYPE && type != Long.class
					&& type != Long.class && type != Short.class) {
				throw new DBOperationException("被遍历的数据主键不是数值型");
			}
			if (this.query == null) {
				this.query = new QueryImpl();
			}

			_initMaxId();

			this.recordQ = new LinkedList<E>();

		} catch (NoSuchFieldException e) {
			throw new DBOperationException("遍历数据失败, clazz " + clazz, e);
		}
	}

	private void _initMaxId() {
		IQuery query = new QueryImpl();
		query.limit(1).orderBy(pkName, Order.DESC);
		List<E> one = null;
		try {
			one = selectGlobalByQuery(this.dbConnInfo.getDatasource().getConnection(), query, clazz);
		} catch (SQLException e1) {
			throw new DBOperationException("获取max id失败");
		}
		if (one.isEmpty()) {
			this.maxId = 0;
		} else {
			E e = one.get(0);
			this.maxId = ReflectUtil.getPkValue(e).longValue();
		}

		LOG.info("clazz " + clazz + " maxId " + this.maxId);
	}

	@Override
	public long getCount() {
		return selectGlobalCount(query, dbConnInfo, this.dbConnInfo.getClusterName(), clazz).longValue();
	}

	@Override
	public boolean hasNext() {
		if (this.recordQ.isEmpty()) {
			IQuery query = this.query.clone();
			long high = this.latestId + STEP;
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
				high = this.latestId + STEP;
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

	@Override
	public E next() {
		return this.recordQ.poll();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("this iterator cann't doing remove");
	}

	@Override
	public void setQuery(IQuery query) {
		if (query != null)
			this.query = query;
	}

}
