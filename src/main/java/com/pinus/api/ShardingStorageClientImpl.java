package com.pinus.api;

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
import org.apache.log4j.Logger;

import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.api.enums.EnumDBRouteAlg;
import com.pinus.api.enums.EnumDbConnectionPoolCatalog;
import com.pinus.api.enums.EnumMode;
import com.pinus.api.query.IQuery;
import com.pinus.api.query.QueryImpl;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.impl.AppDBClusterImpl;
import com.pinus.cluster.impl.EnvDBClusterImpl;
import com.pinus.cluster.lock.CuratorDistributeedLock;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlDBClusterConfigImpl;
import com.pinus.constant.Const;
import com.pinus.datalayer.IShardingMasterQuery;
import com.pinus.datalayer.IShardingSlaveQuery;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.IShardingUpdate;
import com.pinus.datalayer.jdbc.FatDB;
import com.pinus.datalayer.jdbc.ShardingMasterQueryImpl;
import com.pinus.datalayer.jdbc.ShardingSlaveQueryImpl;
import com.pinus.datalayer.jdbc.ShardingUpdateImpl;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.LoadConfigException;
import com.pinus.generator.IIdGenerator;
import com.pinus.generator.impl.DistributedSequenceIdGeneratorImpl;
import com.pinus.generator.impl.StandaloneSequenceIdGeneratorImpl;
import com.pinus.util.CheckUtil;
import com.pinus.util.ReflectUtil;
import com.pinus.util.StringUtils;

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
	public static final Logger LOG = Logger.getLogger(ShardingStorageClientImpl.class);

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
	private boolean isCreateTable = false;

	/**
	 * 扫描数据对象的包. 数据对象是使用了@Table注解的javabean.
	 */
	private String scanPackage;

	/**
	 * 数据库集群引用.
	 */
	private IDBCluster dbCluster;

	/**
	 * 主缓存.
	 */
	private IPrimaryCache primaryCache;

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
		// 设置扫描对象的包
		this.dbCluster.setScanPackage(this.scanPackage);
		// 启动集群
		try {
			this.dbCluster.startup();
		} catch (DBClusterException e) {
			throw new RuntimeException(e);
		}

		// 初始化curator framework
		this.curatorClient = CuratorFrameworkFactory.newClient(this.dbCluster.getClusterConfig().getZookeeperUrl(),
				new RetryNTimes(5, 1000));
		this.curatorClient.start();

		// 发现可用的缓存
		if (this.primaryCache != null) {
			StringBuilder memcachedAddressInfo = new StringBuilder();
			Collection<SocketAddress> servers = this.primaryCache.getAvailableServers();
			if (servers != null) {
				for (SocketAddress server : servers) {
					memcachedAddressInfo.append(((InetSocketAddress) server).getAddress().getHostAddress() + ":"
							+ ((InetSocketAddress) server).getPort());
					memcachedAddressInfo.append(",");
				}
				memcachedAddressInfo.deleteCharAt(memcachedAddressInfo.length() - 1);
				LOG.info("find memcached server - " + memcachedAddressInfo.toString());
			}
		}

		// 初始化ID生成器
		if (this.mode == EnumMode.STANDALONE) {
			this.idGenerator = new StandaloneSequenceIdGeneratorImpl(this.dbCluster.getClusterConfig());
		} else if (this.mode == EnumMode.DISTRIBUTED) {
			this.idGenerator = new DistributedSequenceIdGeneratorImpl(this.dbCluster.getClusterConfig(),
					this.curatorClient);
		} else {
			throw new IllegalStateException("运行模式设置错误, mode=" + this.mode);
		}

		//
		// 初始化分库分表增删改查实现.
		//
		this.updater = new ShardingUpdateImpl();
		this.updater.setDBCluster(this.dbCluster);
		this.updater.setPrimaryCache(this.primaryCache);
		this.updater.setIdGenerator(this.idGenerator);

		this.masterQueryer = new ShardingMasterQueryImpl();
		this.masterQueryer.setDBCluster(this.dbCluster);
		this.masterQueryer.setPrimaryCache(this.primaryCache);

		this.slaveQueryer = new ShardingSlaveQueryImpl();
		this.slaveQueryer.setDBCluster(this.dbCluster);
		this.slaveQueryer.setPrimaryCache(this.primaryCache);

		// FashionEntity dependency this.
		instance = this;
	}

	@Override
	public Number globalSave(Object entity) {
		CheckUtil.checkGlobalEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		CheckUtil.checkClusterName(clusterName);

		return this.updater.globalSave(entity, clusterName);
	}

	@Override
	public void globalUpdate(Object entity) {
		CheckUtil.checkGlobalEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		CheckUtil.checkClusterName(clusterName);

		this.updater.globalUpdate(entity, clusterName);
	}

	@Override
	public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkClusterName(clusterName);

		return this.updater.globalSaveBatch(entities, clusterName);
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

	@Override
	public Number getGlobalCount(String clusterName, Class<?> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz);
	}

	@Override
	public Number getGlobalCount(String clusterName, Class<?> clazz, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		if (masterSlave == null) {
			throw new IllegalArgumentException("master slave param cann't be null");
		}

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz);
		default:
			return this.slaveQueryer.getGlobalCountFromSlave(clusterName, clazz, masterSlave);
		}
	}

	@Override
	public Number getGlobalCount(IQuery query, String clusterName, Class<?> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getGlobalCountFromMaster(query, clusterName, clazz);
	}

	@Override
	public Number getGlobalCount(IQuery query, String clusterName, Class<?> clazz, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz);
		default:
			return this.slaveQueryer.getGlobalCountFromSlave(clusterName, clazz, masterSlave);
		}
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findGlobalByPkFromMaster(pk, clusterName, clazz);
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findGlobalByPkFromMaster(pk, clusterName, clazz);
		default:
			return this.slaveQueryer.findGlobalByPkFromSlave(pk, clusterName, clazz, masterSlave);
		}
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz) {
		return this.masterQueryer.findGlobalOneByQueryFromMaster(query, clusterName, clazz);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findGlobalOneByQueryFromMaster(query, clusterName, clazz);
		default:
			return this.slaveQueryer.findGlobalOneByQueryFromSlave(query, clusterName, clazz, masterSlave);
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

	@Override
	public <T> List<T> findGlobalByPks(String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findGlobalByPksFromMaster(clusterName, clazz, pks);
		default:
			return this.slaveQueryer.findGlobalByPksFromSlave(clusterName, clazz, masterSlave, pks);
		}
	}

	@Override
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberList(pks);

		return this.masterQueryer.findGlobalByPksFromMaster(pks, clusterName, clazz);
	}

	@Override
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberList(pks);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findGlobalByPksFromMaster(pks, clusterName, clazz);
		default:
			return this.slaveQueryer.findGlobalByPksFromSlave(pks, clusterName, clazz, masterSlave);
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
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findGlobalByQueryFromMaster(query, clusterName, clazz);
	}

	@Override
	public <T> List<T> findGlobalByQuery(IQuery query, String clusterName, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findGlobalByQueryFromMaster(query, clusterName, clazz);
		default:
			return this.slaveQueryer.findGlobalByQueryFromSlave(query, clusterName, clazz, masterSlave);
		}
	}

	@Override
	public Number getCount(Class<?> clazz) {
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(clazz);
	}

	@Override
	public Number getCount(Class<?> clazz, IQuery query) {
		CheckUtil.checkClass(clazz);
		CheckUtil.checkQuery(query);

		return this.masterQueryer.getCountFromMaster(clazz, query);
	}

	@Override
	public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz) {
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(shardingKey, clazz);
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
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkFromMaster(pk, shardingKey, clazz);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByPkFromMaster(pk, shardingKey, clazz);
		default:
			return this.slaveQueryer.findByPkFromSlave(pk, shardingKey, clazz, masterSlave);
		}
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
		return this.masterQueryer.findOneByQueryFromMaster(query, shardingKey, clazz);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave) {
		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findOneByQueryFromMaster(query, shardingKey, clazz);
		default:
			return this.slaveQueryer.findOneByQueryFromSlave(query, shardingKey, clazz, masterSlave);
		}
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingKey, Class<T> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPksFromMaster(shardingKey, clazz, pks);
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave,
			Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByPksFromMaster(shardingKey, clazz, pks);
		default:
			return this.slaveQueryer.findByPksFromSlave(shardingKey, clazz, masterSlave, pks);
		}
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkListFromMaster(pks, shardingKey, clazz);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByPkListFromMaster(pks, shardingKey, clazz);
		default:
			return this.slaveQueryer.findByPkListFromSlave(pks, shardingKey, clazz, masterSlave);
		}
	}

	@Override
	public <T> List<T> findByShardingPair(List<IShardingKey<?>> shardingKeys, Class<T> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValueList(shardingKeys);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByShardingPairFromMaster(shardingKeys, clazz, pks);
	}

	@Override
	public <T> List<T> findByShardingPair(List<IShardingKey<?>> shardingKeys, Class<T> clazz,
			EnumDBMasterSlave masterSlave, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValueList(shardingKeys);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByShardingPairFromMaster(shardingKeys, clazz, pks);
		default:
			return this.slaveQueryer.findByShardingPairFromSlave(shardingKeys, clazz, masterSlave, pks);
		}
	}

	@Override
	public <T> List<T> findByShardingPair(List<? extends Number> pks, List<IShardingKey<?>> shardingKeys, Class<T> clazz) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValueList(shardingKeys);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByShardingPairFromMaster(pks, shardingKeys, clazz);
	}

	@Override
	public <T> List<T> findByShardingPair(List<? extends Number> pks, List<IShardingKey<?>> shardingKeys,
			Class<T> clazz, EnumDBMasterSlave masterSlave) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValueList(shardingKeys);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByShardingPairFromMaster(pks, shardingKeys, clazz);
		default:
			return this.slaveQueryer.findByShardingPairFromSlave(pks, shardingKeys, clazz, masterSlave);
		}
	}

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
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByQueryFromMaster(query, shardingKey, clazz);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz,
			EnumDBMasterSlave masterSlave) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingKey);
		CheckUtil.checkClass(clazz);

		switch (masterSlave) {
		case MASTER:
			return this.masterQueryer.findByQueryFromMaster(query, shardingKey, clazz);
		default:
			return this.slaveQueryer.findByQueryFromSlave(query, shardingKey, clazz, masterSlave);
		}
	}

	@Override
	public IShardingStatistics getShardingStatistic() {
		throw new UnsupportedOperationException();
	}

	@Override
	public IDBCluster getDbCluster() {
		return this.dbCluster;
	}

	@Override
	public int genClusterUniqueIntId(String clusterName, String name) {
		return this.idGenerator.genClusterUniqueIntId(clusterName, name);
	}

	@Override
	public long genClusterUniqueLongId(String clusterName, String name) {
		return this.idGenerator.genClusterUniqueLongId(clusterName, name);
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize) {
		return this.idGenerator.genClusterUniqueLongIdBatch(clusterName, name, batchSize);
	}

	@Override
	public int[] genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize) {
		return this.idGenerator.genClusterUniqueIntIdBatch(clusterName, name, batchSize);
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
	public <T> List<FatDB<T>> getAllFatDB(Class<T> clazz, EnumDBMasterSlave masterSlave) {
		List<FatDB<T>> fatDbs = new ArrayList<FatDB<T>>();

		switch (masterSlave) {
		case MASTER:
			FatDB<T> fatDb = null;
			for (DB db : this.dbCluster.getAllMasterShardingDB(clazz)) {
				fatDb = new FatDB<T>();
				fatDb.setPrimaryCache(primaryCache);
				fatDb.setClazz(clazz);
				fatDb.setDb(db);
				fatDbs.add(fatDb);
			}
			break;
		default:
			for (DB db : this.dbCluster.getAllSlaveShardingDB(clazz, masterSlave)) {
				fatDb = new FatDB<T>();
				fatDb.setPrimaryCache(primaryCache);
				fatDb.setClazz(clazz);
				fatDb.setDb(db);
				fatDbs.add(fatDb);
			}
		}

		return fatDbs;
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

}
