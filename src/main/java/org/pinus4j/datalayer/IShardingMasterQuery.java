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

import org.pinus4j.api.IShardingKey;
import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.exceptions.DBOperationException;

/**
 * 主库查询接口.
 * 
 * @author duanbn
 */
public interface IShardingMasterQuery extends IDataQuery {

	/**
	 * 获取集群总数.
	 * 
	 * @param clazz
	 * @return
	 */
	public Number getCountFromMaster(Class<?> clazz, boolean useCache);

	/**
	 * 根据查询条件获取集群记录数.
	 * 
	 * @param clazz
	 *            实体对象
	 * @param query
	 *            查询条件
	 * 
	 * @return 集群记录数
	 */
	public Number getCountFromMaster(Class<?> clazz, IQuery query);

	/**
	 * 获取分库分表记录总数.
	 * 
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象
	 * 
	 * @return 表记录总数
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public Number getCountFromMaster(IShardingKey<?> shardingValue, Class<?> clazz, boolean useCache);

	/**
	 * 根据查询条件获取某一个分片的记录数.
	 */
	public Number getCountFromMaster(IQuery query, IShardingKey<?> shardingValue, Class<?> clazz);

	/**
	 * 一个主分库分表, 根据主键查询. 查询不到则返回null
	 * 
	 * @param pk
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * 
	 * @return 查询结果，找不到返回null
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> T findByPkFromMaster(Number pk, IShardingKey<?> shardingValue, Class<T> clazz, boolean useCache);

	/**
	 * 根据查询条件获取一条数据. 如果查询到多条则返回第一条.当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param query
	 * @param shardingValue
	 * @param clazz
	 * @return 查询结果，找不到返回null
	 */
	public <T> T findOneByQueryFromMaster(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz, boolean useCache);

	/**
	 * 一个主分库分表, 根据多个主键查询.当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param pks
	 *            主键
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByPksFromMaster(IShardingKey<?> shardingValue, Class<T> clazz, Number... pks);

	public <T> List<T> findByPksFromMaster(IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache, Number... pks);

	/**
	 * 一个主分库分表, 根据多个主键查询.当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param pks
	 *            主键数组
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByPkListFromMaster(List<? extends Number> pks, IShardingKey<?> shardingValue,
			Class<T> clazz, boolean useCache);

	/**
	 * 一个主分库分表, 根据条件查询.当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param sql
	 *            查询语句
	 * @param shardingValue
	 *            分库分表因子
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public List<Map<String, Object>> findBySqlFromMaster(SQL sql, IShardingKey<?> shardingValue);

	/**
	 * 根据查询条件对象进行查询.当查询不到数据时返回空的List，不会返回null.
	 * 
	 * @param query
	 *            查询条件
	 * @param shardingValue
	 *            分库分表因子
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByQueryFromMaster(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz,
			boolean useCache);

}
