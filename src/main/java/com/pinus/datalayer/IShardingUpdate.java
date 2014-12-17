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

package com.pinus.datalayer;

import java.util.List;

import com.pinus.api.IShardingKey;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cache.ISecondCache;
import com.pinus.cluster.IDBCluster;
import com.pinus.exception.DBOperationException;
import com.pinus.generator.IIdGenerator;

/**
 * 数据库增删改查操作接口.
 * 
 * @author duanbn
 */
public interface IShardingUpdate {

	/**
	 * 设置ID生成器.
	 * 
	 * @param idGenerator
	 */
	public void setIdGenerator(IIdGenerator idGenerator);

	/**
	 * 保存数据到集群全局库.
	 * 
	 * @param entity
	 *            数据对象
	 * @param clusterName
	 *            集群名
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public Number globalSave(Object entity, String clusterName);

	/**
	 * 批量保存数据到全局库.
	 * 
	 * @param entities
	 *            批量数据对象
	 * @param clusterName
	 *            集群名
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName);

	/**
	 * 更新全局库
	 * 
	 * @param entity
	 *            数据对象
	 * @param clusterName
	 *            集群名称
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void globalUpdate(Object entity, String clusterName);

	/**
	 * 批量更新全局库
	 * 
	 * @param entities
	 *            批量更新数据
	 * @param clusterName
	 *            集群名
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void globalUpdateBatch(List<? extends Object> entities, String clusterName);

	/**
	 * 删除全局库
	 * 
	 * @param pk
	 * @param shardingValue
	 * @param clazz
	 */
	public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName);

	/**
	 * 批量删除全局库
	 * 
	 * @param pks
	 * @param shardingValue
	 * @param clazz
	 */
	public void globalRemoveByPks(List<? extends Number> pks, Class<?> clazz, String clusterName);

	/**
	 * 保存数据. 当实体对象中表示主键的字段有值时，则使用此值作为主键. 否则根据数据库设置 自动生成主键
	 * 
	 * @param entity
	 *            数据对象
	 * @param shardingValue
	 *            分库分表因子
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public Number save(Object entity, IShardingKey<?> shardingValue);

	/**
	 * 批量保存数据.
	 * 
	 * @param entities
	 *            批量数据对象
	 * @param shardingValue
	 *            分库分表因子
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingValue);

	/**
	 * 更新数据. <b>忽略空值的更新</b>
	 * 
	 * @param entity
	 *            数据对象
	 * @param shardingValue
	 *            分库分表因子
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void update(Object entity, IShardingKey<?> shardingValue);

	/**
	 * 单数据库多数据批量更新.
	 * 
	 * @param entities
	 *            批量更新数据
	 * @param shardingValue
	 *            分库分表因子
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingValue);

	/**
	 * 根据主键删除数据.
	 * 
	 * @param pk
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象class
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void removeByPk(Number pk, IShardingKey<?> shardingValue, Class<?> clazz);

	/**
	 * 根据主键批量删除数据.
	 * 
	 * @param pks
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象class
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void removeByPks(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<?> clazz);

	/**
	 * 设置数据库集群.
	 */
	public void setDBCluster(IDBCluster dbCluster);

	/**
	 * 设置一级缓存.
	 */
	public void setPrimaryCache(IPrimaryCache primaryCache);

	/**
	 * 设置二级缓存.
	 * 
	 * @param secondCache
	 */
	public void setSecondCache(ISecondCache secondCache);
}
