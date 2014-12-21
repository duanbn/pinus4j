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

package org.pinus.datalayer.jdbc;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.pinus.api.query.Condition;
import org.pinus.api.query.IQuery;
import org.pinus.api.query.Order;
import org.pinus.api.query.QueryImpl;
import org.pinus.cluster.DB;
import org.pinus.datalayer.IRecordReader;
import org.pinus.exception.DBOperationException;
import org.pinus.util.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 某个实体对象的一个分表遍历器. <b>需要注意的是此遍历器只能遍历主键是数值型的实体</b>
 * 
 * @author duanbn
 * 
 */
public class ShardingRecordReader<E> extends AbstractShardingQuery implements
		IRecordReader<E> {

	public static final Logger LOG = LoggerFactory
			.getLogger(ShardingRecordReader.class);

	private Class<E> clazz;

	private IQuery query;

	private DB db;

	private String pkName;
	private Queue<E> recordQ;
	private static final int STEP = 2000;
	private long latestId = 0;
	private long maxId;

	public ShardingRecordReader(DB db, Class<E> clazz) {
		this.db = db;
		this.clazz = clazz;

		try {
			// check pk type
			pkName = ReflectUtil.getPkName(clazz);
			Class<?> type = clazz.getDeclaredField(pkName).getType();
			if (type != Long.TYPE && type != Integer.TYPE && type != Short.TYPE
					&& type != Long.class && type != Long.class
					&& type != Short.class) {
				throw new DBOperationException("被遍历的数据主键不是数值型");
			}
			if (this.query == null) {
				this.query = new QueryImpl();
			}

			_initMaxId();

			this.recordQ = new LinkedList<E>();

		} catch (NoSuchFieldException e) {
			throw new DBOperationException("遍历数据失败, clazz " + clazz + " " + db,
					e);
		}
	}

	private void _initMaxId() {
		IQuery query = new QueryImpl();
		query.limit(1).orderBy(pkName, Order.DESC);
		List<E> one = selectByQuery(db, query, clazz);
		if (one.isEmpty()) {
			this.maxId = 0;
		} else {
			E e = one.get(0);
			this.maxId = ReflectUtil.getPkValue(e).longValue();
		}

		LOG.info("clazz " + clazz + " DB " + db + " maxId " + this.maxId);
	}

	@Override
	public long getCount() {
		return selectCount(db, clazz, query).longValue();
	}

	@Override
	public boolean hasNext() {
		if (this.recordQ.isEmpty()) {
			IQuery query = this.query.clone();
			long high = this.latestId + STEP;
			query.add(Condition.gte(pkName, latestId)).add(
					Condition.lt(pkName, high));
			List<E> recrods = selectByQuery(db, query, clazz);
			this.latestId = high;

			while (recrods.isEmpty() && this.latestId < maxId) {
				query = this.query.clone();
				high = this.latestId + STEP;
				query.add(Condition.gte(pkName, this.latestId)).add(
						Condition.lt(pkName, high));
				recrods = selectByQuery(db, query, clazz);
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
		throw new UnsupportedOperationException(
				"this iterator cann't doing remove");
	}

	public IQuery getQuery() {
		return query;
	}

	@Override
	public void setQuery(IQuery query) {
		if (query != null)
			this.query = query;
	}

}
