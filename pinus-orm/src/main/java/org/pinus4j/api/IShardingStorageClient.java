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

package org.pinus4j.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.generator.IIdGenerator;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskFuture;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * Pinus存储中间件用户调用接口. 所有分布式存储的操作都有此接口提供.
 * 
 * replace by PinusClient.
 * 
 * @author duanbn
 */
@Deprecated
public interface IShardingStorageClient {

	/**********************************************************
	 * 事务相关
	 *********************************************************/
	/**
	 * default transaction isolation level is read_commited
	 */
	void beginTransaction();

	void beginTransaction(EnumTransactionIsolationLevel txLevel);

	void commit();

	void rollback();

	/**********************************************************
	 * 数据处理相关
	 *********************************************************/
	/**
	 * 提交一个数据处理任务.
	 * 
	 * @param task
	 *            处理任务
	 * @param clazz
	 *            数据对象的Class
	 * @return
	 */
	<T> TaskFuture submit(ITask<T> task, Class<T> clazz);

	/**
	 * 提交一个数据处理任务. 可以设置一个查询条件，只处理符合查询条件的数据
	 * 
	 * @param task
	 *            处理任务
	 * @param clazz
	 *            数据对象的Class
	 * @param query
	 *            查询条件
	 * @return
	 */
	<T> TaskFuture submit(ITask<T> task, Class<T> clazz, IQuery query);

	/**********************************************************
	 * update相关
	 *********************************************************/
	//
	// global
	//
	/**
	 * 保存数据到全局表.
	 * 
	 * @param entity
	 *            数据对象
	 * @return 新产生的主键
	 * @throws DBOperationException
	 *             操作失败
	 */
	Number globalSave(Object entity);

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
	Number[] globalSaveBatch(List<? extends Object> entities, String clusterName);

	/**
	 * 更新全局表数据.
	 * 
	 * @param entity
	 *            数据对象.
	 * @throws DBOperationException
	 *             操作失败
	 */
	void globalUpdate(Object entity);

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
	void globalUpdateBatch(List<? extends Object> entities, String clusterName);

	/**
	 * 删除全局库
	 * 
	 * @param pk
	 * @param shardingKey
	 * @param clazz
	 */
	void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName);

	/**
	 * 批量删除全局库
	 * 
	 * @param pks
	 * @param shardingKey
	 * @param clazz
	 */
	void globalRemoveByPkList(List<? extends Number> pks, Class<?> clazz, String clusterName);

	/**
	 * 根据主键删除全局库中的记录.
	 * 
	 * @param clusterName
	 *            集群名称
	 * @param clazz
	 *            数据对象
	 * @param pks
	 *            主键
	 */
	void globalRemoveByPks(String clusterName, Class<?> clazz, Number... pks);

	//
	// sharding
	//
	/**
	 * 保存数据到分库分表.
	 * 
	 * @param entity
	 *            数据对象
	 * @return 新产生的主键
	 * @throws DBOperationException
	 *             操作失败
	 */
	Number save(Object entity);

	/**
	 * 批量保存数据.
	 * 
	 * @param entities
	 *            批量数据对象
	 * @param shardingKey
	 *            分库分表因子
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey);

	/**
	 * 更新分库分表数据. 会忽略更新null值和默认值
	 * 
	 * @param entity
	 *            数据对象
	 * @throws DBOperationException
	 *             操作失败
	 */
	void update(Object entity);

	/**
	 * 单数据库多数据批量更新. 会忽略更新null值和默认值
	 * 
	 * @param entities
	 *            批量更新数据
	 * @param shardingKey
	 *            分库分表因子
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey);

	/**
	 * 根据主键删除数据.
	 * 
	 * @param pk
	 *            主键
	 * @param shardingKey
	 *            分库分表因子
	 * @param clazz
	 *            数据对象class
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz);

	/**
	 * 根据主键批量删除数据.
	 * 
	 * @param pks
	 *            主键
	 * @param shardingKey
	 *            分库分表因子
	 * @param clazz
	 *            数据对象class
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	void removeByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<?> clazz);

	/**
	 * 根据主键删除数据.
	 * 
	 * @param shardingKey
	 *            数据分片因子
	 * @param clazz
	 *            数据对象
	 * @param pks
	 *            主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	void removeByPks(IShardingKey<?> shardingKey, Class<?> clazz, Number... pks);

	/**********************************************************
	 * query相关
	 *********************************************************/
	//
	// global and sharding
	//
    /**
     * 查询实体的count数，如果是分片实体会遍历所有的分片求总数.
     */
	Number getCount(Class<?> clazz);

	Number getCount(Class<?> clazz, boolean useCache);

	Number getCount(Class<?> clazz, EnumDBMasterSlave masterSlave);

	Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	Number getCount(IShardingKey<?> shardingKey, Class<?> clazz);

	Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache);

	Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, EnumDBMasterSlave master);

	Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache, EnumDBMasterSlave master);

	Number getCountByQuery(Class<?> clazz, IQuery query);

	Number getCountByQuery(Class<?> clazz, IQuery query, boolean useCache);

	Number getCountByQuery(Class<?> clazz, IQuery query, EnumDBMasterSlave masterSlave);

	Number getCountByQuery(Class<?> clazz, IQuery query, boolean useCache, EnumDBMasterSlave masterSlave);

	Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz);

	Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache);

	Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, EnumDBMasterSlave masterSlave);

	Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> T findByPk(Number pk, Class<T> clazz);

	<T> T findByPk(Number pk, Class<T> clazz, boolean useCache);

	<T> T findByPk(Number pk, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> T findByPk(Number pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz);

	<T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, boolean useCache);

	<T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, Class<T> clazz);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    /**
     * 根据IQuery条件进行查询
     *
     * @param query 查询条件，null表示查询整张表.
     */
	<T> List<T> findByQuery(IQuery query, Class<T> clazz);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz);

	List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz, EnumDBMasterSlave masterSlave);

	<T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz);

	<T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz);

	<T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz);

	<T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz);

	<T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave);

	<T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey);

	List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave);

	/**********************************************************
	 * other相关
	 *********************************************************/
	/**
	 * 创建一个分布式锁.
	 * 
	 * @param lockName
	 *            锁名称
	 * @return
	 */
	Lock createLock(String lockName);

	/**
	 * 设置ID生成器.
	 * 
	 * @param idGenerator
	 */
	void setIdGenerator(IIdGenerator idGenerator);

	/**
	 * 获取ID生成器
	 * 
	 * @return ID生成器
	 */
	IIdGenerator getIdGenerator();

	/**
	 * 获取当前使用的数据库集群.
	 * 
	 * @return 数据库集群
	 */
	IDBCluster getDBCluster();

	/**
	 * 生成全局唯一的int id. 对一个数据对象的集群全局唯一id.
	 * 
	 * @param name
	 * 
	 * @return 单个数据对象的集群全局唯一id
	 * 
	 * @throws DBOperationException
	 *             生成id失败
	 */
	int genClusterUniqueIntId(String name);

	/**
	 * 生成全局唯一的long id. 对一个数据对象的集群全局唯一id.
	 * 
	 * @param clusterName
	 *            数据库集群名
	 * @param clazz
	 *            数据对象class
	 * 
	 * @return 单个数据对象的集群全局唯一id
	 * 
	 * @throws DBOperationException
	 *             生成id失败
	 */
	long genClusterUniqueLongId(String name);

	/**
	 * 批量生成全局唯一主键.
	 * 
	 * @param clusterName
	 *            数据库集群名
	 * @param clazz
	 *            数据对象class
	 * @param batchSize
	 *            批量数
	 */
	long[] genClusterUniqueLongIdBatch(String name, int batchSize);

	/**
	 * 批量生成全局唯一主键.
	 * 
	 * @param clusterName
	 *            数据库集群名
	 * @param clazz
	 *            数据对象class
	 * @param batchSize
	 *            批量数
	 */
	int[] genClusterUniqueIntIdBatch(String name, int batchSize);

	/**
	 * 创建一个查询条件对象.
	 * 
	 * @return 查询条件对象
	 */
	IQuery createQuery();

	/**
	 * 设置存储使用的数据库. 默认使用mysql
	 * 
	 * @param enumDb
	 *            数据库枚举
	 */
	void setEnumDb(EnumDB enumDb);

	/**
	 * 初始化集群客户端.
	 */
	void init() throws LoadConfigException;

	/**
	 * 关闭存储.
	 */
	void destroy();

	/**
	 * 设置数据表同步动作.
	 * 
	 * @param syncAction
	 */
	void setSyncAction(EnumSyncAction syncAction);

	/**
	 * 设置扫描的实体对象包. 用户加载分表信息和自动创建数据表.
	 * 
	 * @param scanPackage
	 */
	void setScanPackage(String scanPackage);

}
