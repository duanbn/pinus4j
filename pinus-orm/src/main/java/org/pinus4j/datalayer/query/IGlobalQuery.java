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

package org.pinus4j.datalayer.query;

import java.util.List;
import java.util.Map;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;

/**
 * query global data.
 * 
 * @author duanbn
 *
 */
public interface IGlobalQuery extends IDataQuery {

	Number getCount(Class<?> clazz);

	Number getCount(Class<?> clazz, boolean useCache);

	Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	Number getCountByQuery(IQuery query, Class<?> clazz);

	Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache);

	Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> T getByPk(Number pk, Class<T> clazz);

	<T> T getByPk(Number pk, Class<T> clazz, boolean useCache);

	<T> T getByPk(Number pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pks, Class<T> clazz);

	<T> List<T> findByPkList(List<? extends Number> pks, Class<T> clazz, boolean useCache);

	<T> List<T> findByPkList(List<? extends Number> pks, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, Class<T> clazz);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz);

	List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz, EnumDBMasterSlave masterSlave);

}
