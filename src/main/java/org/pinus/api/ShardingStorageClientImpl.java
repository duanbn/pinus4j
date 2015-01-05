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

package org.pinus.api;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.pinus.api.enums.EnumDB;
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.api.enums.EnumDBRouteAlg;
import org.pinus.api.enums.EnumDbConnectionPoolCatalog;
import org.pinus.api.enums.EnumMode;
import org.pinus.api.enums.EnumSyncAction;
import org.pinus.api.query.IQuery;
import org.pinus.api.query.QueryImpl;
import org.pinus.cache.IPrimaryCache;
import org.pinus.cache.ISecondCache;
import org.pinus.cluster.IDBCluster;
import org.pinus.cluster.impl.AppDBClusterImpl;
import org.pinus.cluster.impl.EnvDBClusterImpl;
import org.pinus.cluster.lock.CuratorDistributeedLock;
import org.pinus.config.IClusterConfig;
import org.pinus.config.impl.XmlDBClusterConfigImpl;
import org.pinus.constant.Const;
import org.pinus.datalayer.IShardingMasterQuery;
import org.pinus.datalayer.IShardingSlaveQuery;
import org.pinus.datalayer.IShardingUpdate;
import org.pinus.datalayer.jdbc.JdbcMasterQueryImpl;
import org.pinus.datalayer.jdbc.JdbcSlaveQueryImpl;
import org.pinus.datalayer.jdbc.JdbcUpdateImpl;
import org.pinus.exception.DBClusterException;
import org.pinus.exception.LoadConfigException;
import org.pinus.generator.IIdGenerator;
import org.pinus.generator.impl.DistributedSequenceIdGeneratorImpl;
import org.pinus.generator.impl.StandaloneSequenceIdGeneratorImpl;
import org.pinus.util.CheckUtil;
import org.pinus.util.ReflectUtil;
import org.pinus.util.StringUtils;
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
	 * 运行模式. 默认是单机模式.
	 */
	private EnumMode mode = EnumMode.STANDALONE;

	/**
	 * 分片路由算法.
	 */
	private EnumDBRouteAlg enumDBRouteAlg = EnumDBRouteAlg.SIMPLE_HASH;

	/**
	 * 数据库类型.
	 */
	private EnumDB enumDb = EnumDB.MYSQL;

	/**
	 * 是否生成数据库表. 默认是不自动生成库表
	 */
	private boolean isCreateTable = true;

	/**
	 * 同步数据表操作.
	 */
	private EnumSyncAction syncAction = EnumSyncAction.CREATE;

	/**
	 * 扫描数据对象的包. 数据对象是使用了@Table注解的javabean.
	 */
	private String scanPackage;

	/**
	 * 数据库集群引用.
	 */
	private IDBCluster dbCluster;

	/**
	 * 一级缓存.
	 */
	private IPrimaryCache primaryCache;

	/**
	 * 二级缓存.
	 */
	private ISecondCache secondCache;

	/**
	 * 主键生成器. 默认使用SimpleIdGeneratorImpl生成器.
	 */
	private IIdGenerator idGenerator;

	/**
	 * 分库分表更新实现.
	 */
	private IShardingUpdate updater;
	/**
	 * 主库查询实现.
	 */
	private IShardingMasterQuery masterQueryer;
	/**
	 * 从库查询实现.
	 */
	private IShardingSlaveQuery slaveQueryer;

	/**
	 * curator client.
	 */
	private CuratorFramework curatorClient;

	/**
	 * 初始化方法
	 */
	public void init() throws LoadConfigException {
		IClusterConfig clusterConfig = XmlDBClusterConfigImpl.getInstance();

		try {
			// 创建zookeeper目录
			ZooKeeper zkClient = clusterConfig.getZooKeeper();
			Stat stat = zkClient.exists(Const.ZK_ROOT, false);
			if (stat == null) {
				zkClient.create(Const.ZK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			throw new IllegalStateException("初始化zookeeper根目录失败");
		}

		// 发现可用的一级缓存
		if (this.primaryCache != null) {
			StringBuilder memcachedAddressInfo = new StringBuilder();
			Collection<SocketAddress> servers = this.primaryCache.getAvailableServers();
			if (servers != null && !servers.isEmpty()) {
				for (SocketAddress server : servers) {
					memcachedAddressInfo.append(((InetSocketAddress) server).getAddress().getHostAddress() + ":"
							+ ((InetSocketAddress) server).getPort());
					memcachedAddressInfo.append(",");
				}
				memcachedAddressInfo.deleteCharAt(memcachedAddressInfo.length() - 1);
				LOG.info("find primary cache, expire " + this.primaryCache.getExpire() + ", memcached server - "
						+ memcachedAddressInfo.toString());
			}
		}
		// 发现可用的二级缓存
		if (this.secondCache != null) {
			StringBuilder memcachedAddressInfo = new StringBuilder();
			Collection<SocketAddress> servers = this.secondCache.getAvailableServers();
			if (servers != null && !servers.isEmpty()) {
				for (SocketAddress server : servers) {
					memcachedAddressInfo.append(((InetSocketAddress) server).getAddress().getHostAddress() + ":"
							+ ((InetSocketAddress) server).getPort());
					memcachedAddressInfo.append(",");
				}
				memcachedAddressInfo.deleteCharAt(memcachedAddressInfo.length() - 1);
				LOG.info("find second cache, expire " + this.secondCache.getExpire() + ", memcached server - "
						+ memcachedAddressInfo.toString());
			}
		}

		// 初始化curator framework
		this.curatorClient = CuratorFrameworkFactory.newClient(clusterConfig.getZookeeperUrl(),
				new RetryNTimes(5, 1000));
		this.curatorClient.start();

		// 初始化ID生成器
		if (this.mode == EnumMode.STANDALONE) {
			this.idGenerator = new StandaloneSequenceIdGeneratorImpl(clusterConfig);
		} else if (this.mode == EnumMode.DISTRIBUTED) {
			this.idGenerator = new DistributedSequenceIdGeneratorImpl(clusterConfig, this.curatorClient);
		} else {
			throw new IllegalStateException("运行模式设置错误, mode=" + this.mode);
		}
		LOG.info("init primary key generator done");

		EnumDbConnectionPoolCatalog enumDbCpCatalog = clusterConfig.getDbConnectionPoolCatalog();

		// 初始化集群
		switch (enumDbCpCatalog) {
		case APP:
			this.dbCluster = new AppDBClusterImpl(enumDb);
			break;
		case ENV:
			this.dbCluster = new EnvDBClusterImpl(enumDb);
			break;
		default:
			this.dbCluster = new AppDBClusterImpl(enumDb);
			break;
		}
		// 设置路由算法.
		this.dbCluster.setDbRouteAlg(this.enumDBRouteAlg);
		// 设置是否生成数据库表
		this.dbCluster.setCreateTable(this.isCreateTable);
		this.dbCluster.setSyncAction(syncAction);
		// 设置扫描对象的包
		this.dbCluster.setScanPackage(this.scanPackage);
		// 启动集群
		try {
			this.dbCluster.startup();
		} catch (DBClusterException e) {
			throw new RuntimeException(e);
		}

		//
		// 初始化分库分表增删改查实现.
		//
		this.updater = new JdbcUpdateImpl();
		this.updater.setDBCluster(this.dbCluster);
		this.updater.setPrimaryCache(this.primaryCache);
		this.updater.setIdGenerator(this.idGenerator);
		this.updater.setSecondCache(secondCache);

		this.masterQueryer = new JdbcMasterQueryImpl();
		this.masterQueryer.setDBCluster(this.dbCluster);
		this.masterQueryer.setPrimaryCache(this.primaryCache);
		this.masterQueryer.setSecondCache(secondCache);

		this.slaveQueryer = new JdbcSlaveQueryImpl();
		this.slaveQueryer.setDBCluster(this.dbCluster);
		this.slaveQueryer.setPrimaryCache(this.primaryCache);
		this.slaveQueryer.setSecondCache(secondCache);

		// FashionEntity dependency this.
		instance = this;
	}

	// ////////////////////////////////////////////////////////
	// 数据处理相关
	// ////////////////////////////////////////////////////////
	@Override
	public <T> TaskFuture submit(ITask<T> task, Class<T> clazz) {
		TaskExecutor<T> taskExecutor = new TaskExecutor<T>(clazz, this.dbCluster);
		return taskExecutor.execute(task);
	}

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

		return this.updater.globalSave(entity, clusterName);
	}

	@Override
	public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkClusterName(clusterName);

		return this.updater.globalSaveBatch(entities, clusterName);
	}

	@Override
	public void globalUpdate(Object entity) {
		CheckUtil.checkGlobalEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		CheckUtil.checkClusterName(clusterName);

		this.updater.globalUpdate(entity, clusterName);
	}

	@Override
	public void globalUpdateBatch(List<? extends Object> entities, String clusterName) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkClusterName(clusterName);

		this.updater.globalUpdateBatch(entities, clusterName);
	}

	@Override
	public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkClusterName(clusterName);

		this.updater.globalRemoveByPk(pk, clazz, clusterName);
	}

	@Override
	public void globalRemoveByPkList(List<? extends Number> pks, Class<?> clazz, String clusterName) {
		if (pks == null || pks.isEmpty()) {
			return;
		}
		CheckUtil.checkClass(clazz);
		CheckUtil.checkClusterName(clusterName);

		this.updater.globalRemoveByPks(pks, clazz, clusterName);
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

		return this.updater.save(entity, sk);
	}

	@Override
	public void update(Object entity) {
		CheckUtil.checkShardingEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		Object shardingKey = ReflectUtil.getShardingValue(entity);
		IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingKey);
		CheckUtil.checkShardingValue(sk);

		this.updater.update(entity, sk);
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingKey);

		return this.updater.saveBatch(entities, shardingKey);
	}

	@Override
	public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingKey);

		this.updater.updateBatch(entities, shardingKey);
	}

	@Override
	public void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		this.updater.removeByPk(pk, shardingKey, clazz);
	}

	@Override
	public void removeByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<?> clazz) {
		if (pks == null || pks.isEmpty()) {
			return;
		}
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		this.updater.removeByPks(pks, shardingKey, clazz);
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

		return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz, useCache);
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
			return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz, useCache);
		default:
			return this.slaveQueryer.getGlobalCountFromSlave(clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public Number getGlobalCount(IQuery query, String clusterName, Class<?> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getGlobalCountFromMaster(query, clusterName, clazz);
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
			return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz, useCache);
		default:
			return this.slaveQueryer.getGlobalCountFromSlave(clusterName, clazz, useCache, masterSlave);
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

		return this.masterQueryer.findGlobalByPkFromMaster(pk, clusterName, clazz, useCache);
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
			return this.masterQueryer.findGlobalByPkFromMaster(pk, clusterName, clazz, useCache);
		default:
			return this.slaveQueryer.findGlobalByPkFromSlave(pk, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz) {
		return findGlobalOneByQuery(query, clusterName, clazz, true);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz, boolean useCache) {
		return this.masterQueryer.findGlobalOneByQueryFromMaster(query, clusterName, clazz, useCache);
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
			return this.masterQueryer.findGlobalOneByQueryFromMaster(query, clusterName, clazz, useCache);
		default:
			return this.slaveQueryer.findGlobalOneByQueryFromSlave(query, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findGlobalByPksFromMaster(clusterName, clazz, pks);
	}

	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, boolean useCache, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findGlobalByPksFromMaster(clusterName, clazz, useCache, pks);
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
			return this.masterQueryer.findGlobalByPksFromMaster(clusterName, clazz, useCache, pks);
		default:
			return this.slaveQueryer.findGlobalByPksFromSlave(clusterName, clazz, masterSlave, useCache, pks);
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

		return this.masterQueryer.findGlobalByPkListFromMaster(pks, clusterName, clazz, useCache);
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
			return this.masterQueryer.findGlobalByPkListFromMaster(pks, clusterName, clazz, useCache);
		default:
			return this.slaveQueryer.findGlobalByPkListFromSlave(pks, clusterName, clazz, useCache, masterSlave);
		}
	}

	@Override
	public List<Map<String, Object>> findGlobalBySql(SQL sql, String clusterName) {
		CheckUtil.checkSQL(sql);
		CheckUtil.checkClusterName(clusterName);

		return this.masterQueryer.findGlobalBySqlFromMaster(sql, clusterName);
	}

	@Override
	public List<Map<String, Object>> findGlobalBySql(SQL sql, String clusterName, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkSQL(sql);
		CheckUtil.checkClusterName(clusterName);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findGlobalBySqlFromMaster(sql, clusterName);
		default:
			return this.slaveQueryer.findGlobalBySqlFromSlave(sql, clusterName, masterSlave);
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

		return this.masterQueryer.findGlobalByQueryFromMaster(query, clusterName, clazz, useCache);
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
			return this.masterQueryer.findGlobalByQueryFromMaster(query, clusterName, clazz, useCache);
		default:
			return this.slaveQueryer.findGlobalByQueryFromSlave(query, clusterName, clazz, useCache, masterSlave);
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
	public IDBCluster getDbCluster() {
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
		InterProcessMutex curatorLock = new InterProcessMutex(curatorClient, Const.ZK_LOCKS + "/" + lockName);
		return new CuratorDistributeedLock(curatorLock);
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

	public EnumDBRouteAlg getEnumDBRouteAlg() {
		return enumDBRouteAlg;
	}

	@Override
	public void setEnumDBRouteAlg(EnumDBRouteAlg enumDBRouteAlg) {
		if (enumDBRouteAlg == null) {
			throw new IllegalArgumentException("参数错误, 参数不能为空");
		}
		this.enumDBRouteAlg = enumDBRouteAlg;
	}

	@Override
	public void destroy() {
		// close database cluster.
		try {
			this.dbCluster.shutdown();
		} catch (DBClusterException e) {
			throw new RuntimeException(e);
		}

		// close id generator
		this.idGenerator.close();

		// close curator
		CloseableUtils.closeQuietly(this.curatorClient);
	}

	@Override
	public void setMode(EnumMode mode) {
		if (mode != null) {
			this.mode = mode;
		}
	}

	public boolean isCreateTable() {
		return isCreateTable;
	}

	public EnumSyncAction getSyncAction() {
		return syncAction;
	}

	@Override
	public void setSyncAction(EnumSyncAction syncAction) {
		this.syncAction = syncAction;
	}

	@Override
	public void setCreateTable(boolean isCreateTable) {
		this.isCreateTable = isCreateTable;
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

	@Override
	public void setPrimaryCache(IPrimaryCache primaryCache) {
		this.primaryCache = primaryCache;
	}

	@Override
	public void setSecondCache(ISecondCache secondCache) {
		this.secondCache = secondCache;
	}

}
