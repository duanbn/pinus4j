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

package org.pinus.datalayer;

import java.util.List;

import org.pinus.api.SQL;
import org.pinus.api.query.IQuery;

/**
 * global master database query interface.
 *
 * @author duanbn
 * @since 0.7.1
 */
public interface IGlobalMasterQuery extends IDataQuery {

    /**
	 * 查询全局库表的数量.
	 * 
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            表示全局表的实体对象
	 * @return count数
	 */
	public Number getGlobalCountFromMaster(String clusterName, Class<?> clazz, boolean useCache);

	/**
	 * 
	 * @param query
	 * @param clusterName
	 * @param clazz
	 * @return
	 */
	public Number getGlobalCountFromMaster(IQuery query, String clusterName, Class<?> clazz);

	/**
	 * 根据pk查询全局表中的数据. 查询不到则返回null
	 * 
	 * @param pk
	 *            主键
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            实体对象
	 * @return 数据
	 */
	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz);

	public <T> T findGlobalByPkFromMaster(Number pk, String clusterName, Class<T> clazz, boolean useCache);

	/**
	 * 根据Query对象查询全局表数据. 查询不到则返回null
	 * 
	 * @param query
	 *            Query条件
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            实体对象
	 * @return 数据
	 */
	public <T> T findGlobalOneByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz, boolean useCache);

	/**
	 * 根据主键查询全局表数据. 当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            实体对象
	 * @param pks
	 *            主键
	 * @return 数据
	 */
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, Number... pks);
	
	public <T> List<T> findGlobalByPksFromMaster(String clusterName, Class<T> clazz, boolean useCache, Number... pks);

	/**
	 * 根据主键查询全局表数据. 当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            实体对象
	 * @param pks
	 *            主键
	 * @return 数据
	 */
	public <T> List<T> findGlobalByPkListFromMaster(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache);

	/**
	 * 根据sql查询全局表. 当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param sql
	 *            查询语句
	 * @param clusterName
	 *            集群名
	 * @return 数据
	 */
	public <T> List<T> findGlobalBySqlFromMaster(SQL sql, String clusterName);

	/**
	 * 根据Query查询全局表. 当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param query
	 *            Query对象
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            实体对象
	 * @return 数据
	 */
	public <T> List<T> findGlobalByQueryFromMaster(IQuery query, String clusterName, Class<T> clazz, boolean useCache);

}
