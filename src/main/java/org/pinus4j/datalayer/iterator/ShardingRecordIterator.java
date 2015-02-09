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

import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.Order;
import org.pinus4j.api.query.QueryImpl;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 某个实体对象的一个分表遍历器. <b>需要注意的是此遍历器只能遍历主键是数值型的实体</b>
 * 
 * @author duanbn
 * 
 */
public class ShardingRecordIterator<E> extends AbstractRecordIterator<E> {

	public static final Logger LOG = LoggerFactory.getLogger(ShardingRecordIterator.class);

	private ShardingDBResource db;

	public ShardingRecordIterator(ShardingDBResource db, Class<E> clazz) {
		super(clazz);

		this.db = db;

		this.maxId = getMaxId();
	}

	public long getMaxId() {
		long maxId = 0;

		IQuery query = new QueryImpl();
		query.limit(1).orderBy(pkName, Order.DESC);
		List<E> one;
		try {
			one = selectByQuery(db, query, clazz);
		} catch (SQLException e1) {
			throw new DBOperationException(e1);
		}
		if (!one.isEmpty()) {
			E e = one.get(0);
			maxId = ReflectUtil.getPkValue(e).longValue();
		}

		LOG.info("clazz " + clazz + " DB " + db + " maxId=" + maxId);

		return maxId;
	}

	@Override
	public long getCount() {
		try {
			return selectCount(db, clazz, query).longValue();
		} catch (SQLException e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public boolean hasNext() {
		if (this.recordQ.isEmpty()) {
			IQuery query = this.query.clone();
			long high = this.latestId + step;
			query.add(Condition.gte(pkName, latestId)).add(Condition.lt(pkName, high));
			try {
				List<E> recrods = selectByQuery(db, query, clazz);
				this.latestId = high;

				while (recrods.isEmpty() && this.latestId < maxId) {
					query = this.query.clone();
					high = this.latestId + step;
					query.add(Condition.gte(pkName, this.latestId)).add(Condition.lt(pkName, high));
					recrods = selectByQuery(db, query, clazz);
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
