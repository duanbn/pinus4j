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

package org.pinus4j.datalayer;

import java.util.List;
import java.util.Map;

import org.pinus4j.api.SQL;
import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.api.query.IQuery;

/**
 * global slave database query interface.
 *
 * @author duanbn
 * @since 0.7.1
 */
public interface IGlobalSlaveQuery extends IDataQuery {

    /**
	 * 从从库中获取全局数量.
	 * 
	 * @param clusterName
	 * @param clazz
	 * @param slave
	 * @return
	 */
	public Number getGlobalCountFromSlave(String clusterName, Class<?> clazz, boolean useCache, EnumDBMasterSlave slave);

	public Number getGlobalCountFromSlave(IQuery query, String clusterName, Class<?> clazz, EnumDBMasterSlave slave);

	/**
	 * 根据pk从从库中查询
	 * 
	 * @param pk
	 * @param clusterName
	 * @param clazz
	 * @param slave
	 * @return
	 */
	public <T> T findGlobalByPkFromSlave(Number pk, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave);

	/**
	 * 根据查询条件查询单条数据
	 * 
	 * @param query
	 * @param clusterName
	 * @param clazz
	 * @param slave
	 * @return
	 */
	public <T> T findGlobalOneByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave);

	public <T> List<T> findGlobalByPksFromSlave(String clusterName, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks);

	public <T> List<T> findGlobalByPksFromSlave(String clusterName, Class<T> clazz, EnumDBMasterSlave slave,
			boolean useCache, Number... pks);

	/**
	 * 
	 * @param pks
	 * @param clusterName
	 * @param clazz
	 * @param slave
	 * @return
	 */
	public <T> List<T> findGlobalByPkListFromSlave(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave slave);

	/**
	 * 
	 * @param sql
	 * @param clusterName
	 * @param slave
	 * @return
	 */
	public List<Map<String, Object>> findGlobalBySqlFromSlave(SQL sql, String clusterName, EnumDBMasterSlave slave);

	/**
	 * 
	 * @param query
	 * @param clusterName
	 * @param clazz
	 * @param slave
	 * @return
	 */
	public <T> List<T> findGlobalByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave);

}
