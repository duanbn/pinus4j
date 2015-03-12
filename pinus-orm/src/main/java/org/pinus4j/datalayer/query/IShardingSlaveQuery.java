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
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.exceptions.DBOperationException;

/**
 * 从库查询操作接口.
 * replace by IShardingQuery
 * 
 * @author duanbn
 */
@Deprecated
public interface IShardingSlaveQuery extends IDataQuery {

	/**
	 * 获取从分库分表记录总数.
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
	public Number getCountFromSlave(IShardingKey<?> shardingValue, Class<?> clazz, boolean useCache,
			EnumDBMasterSlave slave);

	/**
	 * 一个从分库分表, 根据主键查询.
	 * 
	 * @param pk
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> T findByPkFromSlave(Number pk, IShardingKey<?> shardingValue, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave);

	/**
	 * 根据查询条件获取一条数据. 如果查询到多条则返回第一条.
	 * 
	 * @param query
	 * @param shardingValue
	 * @param clazz
	 * @param slave
	 * @return 查询结果，找不到返回null
	 */
	public <T> T findOneByQueryFromSlave(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave slave);

	/**
	 * 一个从分库分表, 根据多个主键查询.
	 * 
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
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
	public <T> List<T> findByPksFromSlave(IShardingKey<?> shardingValue, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks);

	public <T> List<T> findByPksFromSlave(IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave slave,
			boolean useCache, Number... pks);

	/**
	 * 一个从分库分表, 根据多个主键查询.
	 * 
	 * @param pks
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByPkListFromSlave(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave slave);

	/**
	 * 一个从分库分表, 根据条件查询.
	 * 
	 * @param sql
	 *            查询语句
	 * @param shardingValue
	 *            分库分表因子
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public List<Map<String, Object>> findBySqlFromSlave(SQL sql, IShardingKey<?> shardingValue, EnumDBMasterSlave slave);

	/**
	 * 根据查询条件对象进行查询.
	 * 
	 * @param query
	 *            查询条件
	 * @param shardingValue
	 *            分库分表因子
	 * @param slave
	 *            从库查询
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByQueryFromSlave(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave slave);

	/**
	 * 设置缓存.
	 */
	public void setPrimaryCache(IPrimaryCache primaryCache);

	/**
	 * 设置二级缓存.
	 * 
	 * @param secondCache
	 */
	public void setSecondCache(ISecondCache secondCache);

}
