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

import java.util.List;

import org.pinus.api.query.IQuery;
import org.pinus.cluster.DB;

/**
 * 分库分表引用，可以直接操作数据库提供增删改查功能.
 * 
 * @author duanbn
 * 
 */
public class FatDB<T> extends AbstractShardingQuery {

	private Class<T> clazz;

	private DB db;

	public List<T> loadByQuery(IQuery query, boolean useCache) {
		List<T> result = null;

		if (isCacheAvailable(clazz, useCache)) {
			Number[] pkValues = selectPksByQuery(db, query, clazz);
			result = selectByPksWithCache(db, clazz, pkValues, useCache);
		} else {
			result = selectByQuery(db, query, clazz);
		}

		return result;
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

	public DB getDb() {
		return db;
	}

	public void setDb(DB db) {
		this.db = db;
	}

}
