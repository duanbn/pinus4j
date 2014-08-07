package com.pinus.api;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBConnect;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.api.enums.EnumDBRouteAlg;
import com.pinus.api.enums.EnumMode;
import com.pinus.api.query.IQuery;
import com.pinus.api.query.QueryImpl;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.impl.DbcpDBClusterImpl;
import com.pinus.cluster.lock.DistributedLock;
import com.pinus.datalayer.IShardingMasterQuery;
import com.pinus.datalayer.IShardingSlaveQuery;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.IShardingUpdate;
import com.pinus.datalayer.jdbc.FatDB;
import com.pinus.datalayer.jdbc.ShardingMasterQueryImpl;
import com.pinus.datalayer.jdbc.ShardingSlaveQueryImpl;
import com.pinus.datalayer.jdbc.ShardingUpdateImpl;
import com.pinus.exception.DBClusterException;
import com.pinus.generator.IIdGenerator;
import com.pinus.generator.impl.DistributedSequenceIdGeneratorImpl;
import com.pinus.generator.impl.StandaloneSequenceIdGeneratorImpl;
import com.pinus.util.CheckUtil;
import com.pinus.util.ReflectUtil;

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
	 * 数据库连接类型. 默认使用DBCP的数据库连接池
	 */
	private EnumDBConnect enumDbConnect = EnumDBConnect.DBCP;

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
	 * 初始化方法
	 */
	public void init() {
		// 初始化集群
		switch (enumDbConnect) {
		case DBCP:
			this.dbCluster = new DbcpDBClusterImpl(enumDb);
			break;
		default:
			this.dbCluster = new DbcpDBClusterImpl(enumDb);
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

		// 初始化ID生成器
		if (this.mode == EnumMode.STANDALONE) {
			this.idGenerator = new StandaloneSequenceIdGeneratorImpl(this.dbCluster.getClusterConfig());
		} else if (this.mode == EnumMode.DISTRIBUTED) {
			this.idGenerator = new DistributedSequenceIdGeneratorImpl(this.dbCluster.getClusterConfig());
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

		// set instance to threadlocal.
		// FashionEntity dependency this.
		storageClientHolder.set(this);
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
		Object shardingValue = ReflectUtil.getShardingValue(entity);
		IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingValue);
		CheckUtil.checkShardingValue(sk);

		return this.updater.save(entity, sk);
	}

	@Override
	public void update(Object entity) {
		CheckUtil.checkShardingEntity(entity);

		String clusterName = ReflectUtil.getClusterName(entity.getClass());
		Object shardingValue = ReflectUtil.getShardingValue(entity);
		IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingValue);
		CheckUtil.checkShardingValue(sk);

		this.updater.update(entity, sk);
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingValue) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingValue);

		return this.updater.saveBatch(entities, shardingValue);
	}

	@Override
	public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingValue) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingValue);

		this.updater.updateBatch(entities, shardingValue);
	}

	@Override
	public void removeByPk(Number pk, IShardingKey<?> shardingValue, Class<?> clazz) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		this.updater.removeByPk(pk, shardingValue, clazz);
	}

	@Override
	public void removeByPkList(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<?> clazz) {
		if (pks == null || pks.isEmpty()) {
			return;
		}
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		this.updater.removeByPks(pks, shardingValue, clazz);
	}

	@Override
	public void removeByPks(IShardingKey<?> shardingValue, Class<?> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return;
		}

		removeByPkList(Arrays.asList(pks), shardingValue, clazz);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz) {
		return this.masterQueryer.findGlobalOneByQueryFromMaster(query, clusterName, clazz);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz) {
		return this.masterQueryer.findOneByQueryFromMaster(query, shardingValue, clazz);
	}

	@Override
	public Number getGlobalCount(String clusterName, Class<?> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getGlobalCountFromMaster(clusterName, clazz);
	}

	@Override
	public Number getGlobalCount(String clusterName, SQL<?> sql) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkSQL(sql);

		return this.masterQueryer.getGlobalCountFromMaster(clusterName, sql);
	}

	@Override
	public <T> T findGlobalByPk(Number pk, String clusterName, Class<T> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findGlobalByPkFromMaster(pk, clusterName, clazz);
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
	public <T> List<T> findGlobalByPkList(List<? extends Number> pks, String clusterName, Class<T> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberList(pks);

		return this.masterQueryer.findGlobalByPksFromMaster(pks, clusterName, clazz);
	}

	@Override
	@Deprecated
	public <T> List<T> findGlobalBySql(SQL<T> sql, String clusterName) {
		CheckUtil.checkSQL(sql);
		CheckUtil.checkClusterName(clusterName);

		return this.masterQueryer.findGlobalBySqlFromMaster(sql, clusterName);
	}

	@Override
	public <T> List<T> findGlobalByQuery(IQuery query, String clusterName, Class<T> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findGlobalByQueryFromMaster(query, clusterName, clazz);
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
	public Number getCount(IShardingKey<?> shardingValue, Class<?> clazz) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(shardingValue, clazz);
	}

	@Override
	public Number getCount(IShardingKey<?> shardingValue, SQL<?> sql) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkSQL(sql);

		return this.masterQueryer.getCountFromMaster(shardingValue, sql);
	}

	@Override
	public Number getCount(IQuery query, IShardingKey<?> shardingValue, Class<?> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(query, shardingValue, clazz);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingKey<?> shardingValue, Class<T> clazz) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkFromMaster(pk, shardingValue, clazz);
	}

	@Override
	public <T> List<T> findByPks(IShardingKey<?> shardingValue, Class<T> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPksFromMaster(shardingValue, clazz, pks);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<T> clazz) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkListFromMaster(pks, shardingValue, clazz);
	}

	@Override
	public <T> List<T> findByShardingPair(List<IShardingKey<?>> shardingValues, Class<T> clazz, Number... pks) {
		if (pks == null || pks.length == 0) {
			return new ArrayList<T>();
		}

		CheckUtil.checkShardingValueList(shardingValues);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByShardingPairFromMaster(shardingValues, clazz, pks);
	}

	@Override
	public <T> List<T> findByShardingPair(List<? extends Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValueList(shardingValues);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByShardingPairFromMaster(pks, shardingValues, clazz);
	}

	@Override
	@Deprecated
	public <T> List<T> findBySql(SQL<T> sql, IShardingKey<?> shardingValue) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkSQL(sql);

		return this.masterQueryer.findBySqlFromMaster(sql, shardingValue);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByQueryFromMaster(query, shardingValue, clazz);
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
		return this.idGenerator.genClusterUniqueIntId(this.dbCluster, clusterName, name);
	}

	@Override
	public long genClusterUniqueLongId(String clusterName, String name) {
		return this.idGenerator.genClusterUniqueLongId(this.dbCluster, clusterName, name);
	}

	@Override
	public long[] genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize) {
		return this.idGenerator.genClusterUniqueLongIdBatch(this.dbCluster, clusterName, name, batchSize);
	}

	@Override
	public int[] genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize) {
		return this.idGenerator.genClusterUniqueIntIdBatch(this.dbCluster, clusterName, name, batchSize);
	}

	@Override
	public IIdGenerator getIdGenerator() {
		return idGenerator;
	}

	@Override
	public Lock createLock(String lockName) {
		return new DistributedLock(lockName, true, this.dbCluster.getClusterConfig());
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

	public EnumDBConnect getEnumDbConnect() {
		return enumDbConnect;
	}

	@Override
	public void setEnumDbConnect(EnumDBConnect enumDbConnect) {
		if (enumDbConnect == null) {
			throw new IllegalArgumentException("参数错误, 参数不能为空");
		}
		this.enumDbConnect = enumDbConnect;
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

		// remove instance from threadlocal.
		storageClientHolder.remove();
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
		this.scanPackage = scanPackage;
	}

	@Override
	public void setPrimaryCache(IPrimaryCache primaryCache) {
		this.primaryCache = primaryCache;
	}

}
