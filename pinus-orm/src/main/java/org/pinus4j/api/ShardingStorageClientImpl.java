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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import javax.transaction.TransactionManager;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.QueryImpl;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.IDBClusterBuilder;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.cluster.impl.DefaultDBCluster;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.IDataLayerBuilder;
import org.pinus4j.datalayer.IGlobalMasterQuery;
import org.pinus4j.datalayer.IGlobalSlaveQuery;
import org.pinus4j.datalayer.IGlobalUpdate;
import org.pinus4j.datalayer.IShardingMasterQuery;
import org.pinus4j.datalayer.IShardingSlaveQuery;
import org.pinus4j.datalayer.IShardingUpdate;
import org.pinus4j.datalayer.jdbc.JdbcDataLayerBuilder;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.generator.IIdGenerator;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskExecutor;
import org.pinus4j.task.TaskFuture;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;
import org.pinus4j.transaction.impl.BestEffortsOnePCJtaTransactionManager;
import org.pinus4j.utils.CheckUtil;
import org.pinus4j.utils.ReflectUtil;
import org.pinus4j.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用户调用接口实现. 数据库类型、数据库连接类型、路由算法可以通过EnumDB、EnumDBConnect、EnumDBRouteAlg枚举进行设置.
 * 默认数据使用Mysql，数据库连接池使用DBCP，路由算法使用取模哈希算法.<br/>
 * 此类是线程安全的.使用时尽量使用一个对象对集群进行操作.可以通过如下方法实例化此对象<br/>
 * </p>
 * 
 * <pre>
 * IShardingStorageClient shardingClient = new ShardingStorageClientImpl();
 * shrdingClient.setScanPackage("...");
 * // 设置缓存
 * // 如果不设置缓存对象则不启用缓存
 * IPrimaryCache primaryCache = new MemCachedPrimaryCacheImpl(memcachedClient);
 * shardingClient.setPrimaryCache(primaryCache);
 * shardingClient.init();
 * ...
 * shardingClient.destroy();
 * </pre>
 * 
 * @author duanbn
 */
public class ShardingStorageClientImpl implements IShardingStorageClient {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(ShardingStorageClientImpl.class);

	/**
	 * reference it self;
	 */
	public static IShardingStorageClient instance;

	/**
	 * 数据库类型.
	 */
	private EnumDB enumDb = EnumDB.MYSQL;

	/**
	 * 同步数据表操作.
	 */
	private EnumSyncAction syncAction = EnumSyncAction.CREATE;

	/**
	 * 扫描数据对象的包. 数据对象是使用了@Table注解的javabean.
	 */
	private String scanPackage;

	/**
	 * id generator.
	 */
	private IIdGenerator idGenerator;

	/**
	 * 数据库集群引用.
	 */
	private IDBCluster dbCluster;

	private TransactionManager txManager;

	/**
	 * global updater.
	 */
	private IGlobalUpdate globalUpdater;
	/**
	 * global master queryer.
	 */
	private IGlobalMasterQuery globalMasterQuery;
	/**
	 * global slave queryer.
	 */
	private IGlobalSlaveQuery globalSlaveQuery;

	/**
	 * 分库分表更新实现.
	 */
	private IShardingUpdate shardingUpdater;
	/**
	 * 主库查询实现.
	 */
	private IShardingMasterQuery masterQueryer;
	/**
	 * 从库查询实现.
	 */
	private IShardingSlaveQuery slaveQueryer;

	/**
	 * 初始化方法
	 */
	public void init() {
		IDBClusterBuilder dbClusterBuilder = new DefaultDBCluster();
		dbClusterBuilder.setScanPackage(this.scanPackage);
		dbClusterBuilder.setSyncAction(this.syncAction);
		dbClusterBuilder.setDbType(this.enumDb);
		this.dbCluster = dbClusterBuilder.build();

		this.idGenerator = this.dbCluster.getIdGenerator();

		this.txManager = this.dbCluster.getTransactionManager();

		//
		// 初始化分库分表增删改查实现.
		//
		IDataLayerBuilder dataLayerBuilder = JdbcDataLayerBuilder.valueOf(dbCluster);
		dataLayerBuilder.setPrimaryCache(this.dbCluster.getPrimaryCache());
		dataLayerBuilder.setSecondCache(this.dbCluster.getSecondCache());

		this.globalUpdater = dataLayerBuilder.buildGlobalUpdate(this.dbCluster.getIdGenerator());
		this.globalMasterQuery = dataLayerBuilder.buildGlobalMasterQuery();
		this.globalSlaveQuery = dataLayerBuilder.buildGlobalSlaveQuery();

		this.shardingUpdater = dataLayerBuilder.buildShardingUpdate(this.dbCluster.getIdGenerator());
		this.masterQueryer = dataLayerBuilder.buildShardingMasterQuery();
		this.slaveQueryer = dataLayerBuilder.buildShardingSlaveQuery();

		// FashionEntity dependency this.
		instance = this;
	}

	// ////////////////////////////////////////////////////////
	// 事务相关
	// ////////////////////////////////////////////////////////
	@Override
	public void beginTransaction() {
		beginTransaction(EnumTransactionIsolationLevel.READ_COMMITTED);
	}

	@Override
	public void beginTransaction(EnumTransactionIsolationLevel txLevel) {
		((BestEffortsOnePCJtaTransactionManager) this.txManager).setTransactionIsolationLevel(txLevel);
		try {
			this.txManager.begin();
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public void commit() {
		try {
			this.txManager.commit();
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public void rollback() {
		try {
			this.txManager.rollback();
		} catch (Exception e) {
			throw new DBOperationException(e);
		}
	}

	// ////////////////////////////////////////////////////////
	// 数据处理相关
	// ////////////////////////////////////////////////////////
	@Override
	public <T> TaskFuture submit(ITask<T> task, Class<T> clazz) {
		TaskExecutor<T> taskExecutor = new TaskExecutor<T>(clazz, this.dbCluster);
		return taskExecutor.execute(task);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> TaskFuture submit(ITask<T> task, Class<T> clazz, IQuery query) {
		TaskExecutor taskExecutor = new TaskExecutor(clazz, this.dbCluster);
		return taskExecutor.execute(task, query);
	}

	// ////////////////////////////////////////////////////////
	// update相关
	// ////////////////////////////////////////////////////////
	@Override
	public Number globalSave(Object entity) {
		CheckUtil.checkGlobalEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		CheckUtil.checkClusterName(clusterName);

		return this.globalUpdater.globalSave(entity, clusterName);
	}

	@Override
	public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkClusterName(clusterName);

		return this.globalUpdater.globalSaveBatch(entities, clusterName);
	}

	@Override
	public void globalUpdate(Object entity) {
		CheckUtil.checkGlobalEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		CheckUtil.checkClusterName(clusterName);

		this.globalUpdater.globalUpdate(entity, clusterName);
	}

	@Override
	public void globalUpdateBatch(List<? extends Object> entities, String clusterName) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkClusterName(clusterName);

		this.globalUpdater.globalUpdateBatch(entities, clusterName);
	}

	@Override
	public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkClusterName(clusterName);

		this.globalUpdater.globalRemoveByPk(pk, clazz, clusterName);
	}

	@Override
	public void globalRemoveByPkList(List<? extends Number> pks, Class<?> clazz, String clusterName) {
		if (pks == null || pks.isEmpty()) {
			return;
		}
		CheckUtil.checkClass(clazz);
		CheckUtil.checkClusterName(clusterName);

		this.globalUpdater.globalRemoveByPks(pks, clazz, clusterName);
	}

	@Override
	public void globalRemoveByPks(String clusterName, Class<?> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return;
		}

		globalRemoveByPkList(Arrays.asList(pks), clazz, clusterName);
	}

	@Override
	public Number save(Object entity) {
		CheckUtil.checkShardingEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		Object shardingKey = ReflectUtil.getShardingValue(entity);
		IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingKey);
		CheckUtil.checkShardingValue(sk);

		return this.shardingUpdater.save(entity, sk);
	}

	@Override
	public void update(Object entity) {
		CheckUtil.checkShardingEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		Object shardingKey = ReflectUtil.getShardingValue(entity);
		IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingKey);
		CheckUtil.checkShardingValue(sk);

		this.shardingUpdater.update(entity, sk);
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingKey);

		return this.shardingUpdater.saveBatch(entities, shardingKey);
	}

	@Override
	public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingKey);

		this.shardingUpdater.updateBatch(entities, shardingKey);
	}

	@Override
	public void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		this.shardingUpdater.removeByPk(pk, shardingKey, clazz);
	}

	@Override
	public void removeByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<?> clazz) {
		if (pks == null || pks.isEmpty()) {
			return;
		}
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		this.shardingUpdater.removeByPks(pks, shardingKey, clazz);
	}

	@Override
	public void removeByPks(IShardingKey<?> shardingKey, Class<?> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return;
		}

		removeByPkList(Arrays.asList(pks), shardingKey, clazz);
	}

	// ////////////////////////////////////////////////////////
	// query相关
	// ////////////////////////////////////////////////////////
	@Override
	public Number getGlobalCount(String clusterName, Class<?> clazz) {
		return getGlobalCount(clusterName, clazz, true);
	}

	@Override
	public Number getGlobalCount(String clusterName, Class<?> clazz, boolean useCache) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.globalMasterQuery.getGlobalCountFromMaster(clusterName, clazz, useCache);
	}

	public Number getGlobalCount(String clusterName, Class<?> clazz, EnumDBMasterSlave masterSlave) {
		return getGlobalCount(clusterName, clazz, true, masterSlave);
	}

	@Override
	public Number getGlobalCount(String clusterName, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		if (masterSlave == null) {
			throw new IllegalArgumentException("master slave param cann't be null");
		}

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.getGlobalCountFromMaster(clusterName, clazz, useCache);
		default:
			return this.globalSlaveQuery.getGlobalCountFromSlave(clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public Number getGlobalCount(IQuery query, String clusterName, Class<?> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.globalMasterQuery.getGlobalCountFromMaster(query, clusterName, clazz);
	}

	public Number getGlobalCount(IQuery query, String clusterName, Class<?> clazz, EnumDBMasterSlave masterSlave) {
		return getGlobalCount(query, clusterName, clazz, true, masterSlave);
	}

	@Override
	public Number getGlobalCount(IQuery query, String clusterName, Class<?> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.getGlobalCountFromMaster(clusterName, clazz, useCache);
		default:
			return this.globalSlaveQuery.getGlobalCountFromSlave(clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz) {
		return findGlobalByPk(pk, clusterName, clazz, true);
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz, boolean useCache) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);

		return this.globalMasterQuery.findGlobalByPkFromMaster(pk, clusterName, clazz, useCache);
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		return findGlobalByPk(pk, clusterName, clazz, true, masterSlave);
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.findGlobalByPkFromMaster(pk, clusterName, clazz, useCache);
		default:
			return this.globalSlaveQuery.findGlobalByPkFromSlave(pk, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz) {
		return findGlobalOneByQuery(query, clusterName, clazz, true);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz, boolean useCache) {
		return this.globalMasterQuery.findGlobalOneByQueryFromMaster(query, clusterName, clazz, useCache);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		return findGlobalOneByQuery(query, clusterName, clazz, true, masterSlave);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.findGlobalOneByQueryFromMaster(query, clusterName, clazz, useCache);
		default:
			return this.globalSlaveQuery
					.findGlobalOneByQueryFromSlave(query, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.globalMasterQuery.findGlobalByPksFromMaster(clusterName, clazz, pks);
	}

	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, boolean useCache, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.globalMasterQuery.findGlobalByPksFromMaster(clusterName, clazz, useCache, pks);
	}

	@Override
	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave, Number... pks) {
		return findGlobalByPks(clusterName, clazz, masterSlave, true, pks);
	}

	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave,
			boolean useCache, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.findGlobalByPksFromMaster(clusterName, clazz, useCache, pks);
		default:
			return this.globalSlaveQuery.findGlobalByPksFromSlave(clusterName, clazz, masterSlave, useCache, pks);
		}
	}

	@Override
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz) {
		return findGlobalByPkList(pks, clusterName, clazz, true);
	}

	@Override
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberList(pks);

		return this.globalMasterQuery.findGlobalByPkListFromMaster(pks, clusterName, clazz, useCache);
	}

	@Override
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz,
			EnumDBMasterSlave masterSlave) {
		return findGlobalByPkList(pks, clusterName, clazz, true, masterSlave);
	}

	@Override
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberList(pks);

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.findGlobalByPkListFromMaster(pks, clusterName, clazz, useCache);
		default:
			return this.globalSlaveQuery.findGlobalByPkListFromSlave(pks, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public List<Map<String, Object>> findGlobalBySql(SQL sql, String clusterName) {
		CheckUtil.checkSQL(sql);
		CheckUtil.checkClusterName(clusterName);

		return this.globalMasterQuery.findGlobalBySqlFromMaster(sql, clusterName);
	}

	@Override
	public List<Map<String, Object>> findGlobalBySql(SQL sql, String clusterName, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkSQL(sql);
		CheckUtil.checkClusterName(clusterName);

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.findGlobalBySqlFromMaster(sql, clusterName);
		default:
			return this.globalSlaveQuery.findGlobalBySqlFromSlave(sql, clusterName, masterSlave);
		}
	}

	@Override
	public <T> List<T> findGlobalByQuery(IQuery query, String clusterName, Class<T> clazz) {
		return findGlobalByQuery(query, clusterName, clazz, true);
	}

	@Override
	public <T> List<T> findGlobalByQuery(IQuery query, String clusterName, Class<T> clazz, boolean useCache) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.globalMasterQuery.findGlobalByQueryFromMaster(query, clusterName, clazz, useCache);
	}

	@Override
	public <T> List<T> findGlobalByQuery(IQuery query, String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		return findGlobalByQuery(query, clusterName, clazz, true, masterSlave);
	}

	@Override
	public <T> List<T> findGlobalByQuery(IQuery query, String clusterName, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.globalMasterQuery.findGlobalByQueryFromMaster(query, clusterName, clazz, useCache);
		default:
			return this.globalSlaveQuery.findGlobalByQueryFromSlave(query, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public Number getCount(Class<?> clazz) {
		return getCount(clazz, true);
	}

	@Override
	public Number getCount(Class<?> clazz, boolean useCache) {
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(clazz, useCache);
	}

	@Override
	public Number getCount(Class<?> clazz, IQuery query) {
		CheckUtil.checkClass(clazz);
		CheckUtil.checkQuery(query);

		return this.masterQueryer.getCountFromMaster(clazz, query);
	}

	@Override
	public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz) {
		return getCount(shardingKey, clazz, true);
	}

	@Override
	public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache) {
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(shardingKey, clazz, useCache);
	}

	@Override
	public Number getCount(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(query, shardingKey, clazz);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz) {
		return findByPk(pk, shardingKey, clazz, true);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkFromMaster(pk, shardingKey, clazz, useCache);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		return findByPk(pk, shardingKey, clazz, true, masterSlave);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByPkFromMaster(pk, shardingKey, clazz, useCache);
		default:
			return this.slaveQueryer.findByPkFromSlave(pk, shardingKey, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
		return findOneByQuery(query, shardingKey, clazz, true);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		return this.masterQueryer.findOneByQueryFromMaster(query, shardingKey, clazz, useCache);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		return findOneByQuery(query, shardingKey, clazz, true, masterSlave);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findOneByQueryFromMaster(query, shardingKey, clazz, useCache);
		default:
			return this.slaveQueryer.findOneByQueryFromSlave(query, shardingKey, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingKey, Class<T> clazz, Number... pks) {
		return findByPks(shardingKey, clazz, true, pks);
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPksFromMaster(shardingKey, clazz, useCache, pks);
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave,
			Number... pks) {
		return findByPks(shardingKey, clazz, masterSlave, true, pks);
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave,
			boolean useCache, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByPksFromMaster(shardingKey, clazz, useCache, pks);
		default:
			return this.slaveQueryer.findByPksFromSlave(shardingKey, clazz, masterSlave, useCache, pks);
		}
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz) {
		return findByPkList(pks, shardingKey, clazz, true);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkListFromMaster(pks, shardingKey, clazz, useCache);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			EnumDBMasterSlave masterSlave) {
		return findByPkList(pks, shardingKey, clazz, true, masterSlave);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByPkListFromMaster(pks, shardingKey, clazz, useCache);
		default:
			return this.slaveQueryer.findByPkListFromSlave(pks, shardingKey, clazz, useCache, masterSlave);
		}
	}

	// @Override
	// public <T> List<T> findByShardingPair(List<IShardingKey<?>> shardingKeys,
	// Class<T> clazz, Number... pks) {
	// if (pks == null || pks.length == 0) {
	// return new ArrayList<T>();
	// }
	//
	// CheckUtil.checkShardingValueList(shardingKeys);
	// CheckUtil.checkClass(clazz);
	//
	// return this.masterQueryer.findByShardingPairFromMaster(shardingKeys,
	// clazz, pks);
	// }

	// @Override
	// public <T> List<T> findByShardingPair(List<IShardingKey<?>> shardingKeys,
	// Class<T> clazz,
	// EnumDBMasterSlave masterSlave, Number... pks) {
	// if (pks == null || pks.length == 0) {
	// return new ArrayList<T>();
	// }
	//
	// CheckUtil.checkShardingValueList(shardingKeys);
	// CheckUtil.checkClass(clazz);
	//
	// switch (masterSlave) {
	// case MASTER:
	// return this.masterQueryer.findByShardingPairFromMaster(shardingKeys,
	// clazz, pks);
	// default:
	// return this.slaveQueryer.findByShardingPairFromSlave(shardingKeys, clazz,
	// masterSlave, pks);
	// }
	// }

	// @Override
	// public <T> List<T> findByShardingPair(List<? extends Number> pks,
	// List<IShardingKey<?>> shardingKeys, Class<T> clazz) {
	// CheckUtil.checkNumberList(pks);
	// CheckUtil.checkShardingValueList(shardingKeys);
	// CheckUtil.checkClass(clazz);
	//
	// return this.masterQueryer.findByShardingPairFromMaster(pks, shardingKeys,
	// clazz);
	// }
	//
	// @Override
	// public <T> List<T> findByShardingPair(List<? extends Number> pks,
	// List<IShardingKey<?>> shardingKeys,
	// Class<T> clazz, EnumDBMasterSlave masterSlave) {
	// CheckUtil.checkNumberList(pks);
	// CheckUtil.checkShardingValueList(shardingKeys);
	// CheckUtil.checkClass(clazz);
	//
	// switch (masterSlave) {
	// case MASTER:
	// return this.masterQueryer.findByShardingPairFromMaster(pks, shardingKeys,
	// clazz);
	// default:
	// return this.slaveQueryer.findByShardingPairFromSlave(pks, shardingKeys,
	// clazz, masterSlave);
	// }
	// }

	@Override
	public List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey) {
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkSQL(sql);

		return this.masterQueryer.findBySqlFromMaster(sql, shardingKey);
	}

	@Override
	public List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkSQL(sql);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findBySqlFromMaster(sql, shardingKey);
		default:
			return this.slaveQueryer.findBySqlFromSlave(sql, shardingKey, masterSlave);
		}
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
		return findByQuery(query, shardingKey, clazz, true);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByQueryFromMaster(query, shardingKey, clazz, useCache);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz,
			EnumDBMasterSlave masterSlave) {
		return findByQuery(query, shardingKey, clazz, true, masterSlave);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByQueryFromMaster(query, shardingKey, clazz, useCache);
		default:
			return this.slaveQueryer.findByQueryFromSlave(query, shardingKey, clazz, useCache, masterSlave);
		}
	}

	@Override
	public IDBCluster getDBCluster() {
		return this.dbCluster;
	}

	@Override
	public int genClusterUniqueIntId(String name) {
		return this.idGenerator.genClusterUniqueIntId(Const.ZK_SEQUENCE, name);
	}

	@Override
	public long genClusterUniqueLongId(String name) {
		return this.idGenerator.genClusterUniqueLongId(Const.ZK_SEQUENCE, name);
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(String name, int batchSize) {
		return this.idGenerator.genClusterUniqueLongIdBatch(Const.ZK_SEQUENCE, name, batchSize);
	}

	@Override
	public int[] genClusterUniqueIntIdBatch(String name, int batchSize) {
		return this.idGenerator.genClusterUniqueIntIdBatch(Const.ZK_SEQUENCE, name, batchSize);
	}

	@Override
	public IIdGenerator getIdGenerator() {
		return idGenerator;
	}

	@Override
	public Lock createLock(String lockName) {
		return this.dbCluster.createLock(lockName);
	}

	@Override
	public void setIdGenerator(IIdGenerator idGenerator) {
		if (idGenerator == null) {
			throw new IllegalArgumentException("参数错误, 参数不能为空");
		}
		this.idGenerator = idGenerator;
	}

	@Override
	public IQuery createQuery() {
		IQuery query = new QueryImpl();
		return query;
	}

	public EnumDB getEnumDb() {
		return enumDb;
	}

	@Override
	public void setEnumDb(EnumDB enumDb) {
		if (enumDb == null) {
			throw new IllegalArgumentException("参数错误, 参数不能为空");
		}
		this.enumDb = enumDb;
	}

	@Override
	public void destroy() {
		// close database cluster.
		try {
			this.dbCluster.shutdown();
		} catch (DBClusterException e) {
			throw new RuntimeException(e);
		}

	}

	public EnumSyncAction getSyncAction() {
		return syncAction;
	}

	@Override
	public void setSyncAction(EnumSyncAction syncAction) {
		this.syncAction = syncAction;
	}

	public String getScanPackage() {
		return scanPackage;
	}

	@Override
	public void setScanPackage(String scanPackage) {
		if (StringUtils.isBlank(scanPackage)) {
			throw new IllegalArgumentException("参数错误，参数不能为空");
		}

		this.scanPackage = scanPackage;
	}

}
