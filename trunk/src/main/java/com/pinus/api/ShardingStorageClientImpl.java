package com.pinus.api;

import java.util.List;

import org.apache.log4j.Logger;

import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBConnect;
import com.pinus.api.enums.EnumDBRouteAlg;
import com.pinus.api.enums.EnumIdGenrator;
import com.pinus.api.enums.EnumMode;
import com.pinus.api.query.IQuery;
import com.pinus.api.query.QueryImpl;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.impl.DbcpDBClusterImpl;
import com.pinus.cluster.route.IClusterRouter;
import com.pinus.cluster.route.impl.SimpleHashClusterRouterImpl;
import com.pinus.datalayer.IShardingMasterQuery;
import com.pinus.datalayer.IShardingSlaveQuery;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.IShardingUpdate;
import com.pinus.datalayer.jdbc.ShardingMasterQueryImpl;
import com.pinus.datalayer.jdbc.ShardingSlaveQueryImpl;
import com.pinus.datalayer.jdbc.ShardingStatisticsImpl;
import com.pinus.datalayer.jdbc.ShardingUpdateImpl;
import com.pinus.exception.DBClusterException;
import com.pinus.generator.IDBGenerator;
import com.pinus.generator.IIdGenerator;
import com.pinus.generator.impl.DBMySqlGeneratorImpl;
import com.pinus.generator.impl.DistributedSequenceIdGeneratorImpl;
import com.pinus.generator.impl.StandaloneSequenceIdGeneratorImpl;
import com.pinus.util.CheckUtil;

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
	 * 数据库类型.
	 */
	private EnumDB enumDb = EnumDB.MYSQL;

	/**
	 * 集群主键生成器. 默认是根据零库的global_id表来生成. 零库中必须存在global_id表
	 */
	private EnumIdGenrator enumIdGenrator = EnumIdGenrator.DB;

	/**
	 * 数据库连接类型. 默认使用DBCP的数据库连接池
	 */
	private EnumDBConnect enumDbConnect = EnumDBConnect.DBCP;

	/**
	 * 路由算法. 默认使用取模哈希算法
	 */
	private EnumDBRouteAlg enumDBRouteAlg = EnumDBRouteAlg.SIMPLE_HASH;

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
	 * 数据库表生成器.
	 */
	private IDBGenerator dbGenerator;

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
		// 初始化ID生成器
		if (this.mode == EnumMode.STANDALONE) {
			switch (enumIdGenrator) {
			case DB:
				this.idGenerator = new StandaloneSequenceIdGeneratorImpl();
				break;
			default:
				this.idGenerator = new StandaloneSequenceIdGeneratorImpl();
				break;
			}
		} else if (this.mode == EnumMode.DISTRIBUTED) {
			switch (enumIdGenrator) {
			case DB:
				this.idGenerator = new DistributedSequenceIdGeneratorImpl();
				break;
			default:
				this.idGenerator = new DistributedSequenceIdGeneratorImpl();
				break;
			}
		} else {
			throw new IllegalStateException("运行模式设置错误, mode=" + this.mode);
		}

		// 初始化集群路由器
		IClusterRouter dbRouter = null;
		switch (enumDBRouteAlg) {
		case SIMPLE_HASH:
			dbRouter = new SimpleHashClusterRouterImpl();
			break;
		default:
			dbRouter = new SimpleHashClusterRouterImpl();
			break;
		}

		// 初始化数据库表生成器
		switch (enumDb) {
		case MYSQL:
			this.dbGenerator = new DBMySqlGeneratorImpl();
			break;
		default:
			this.dbGenerator = new DBMySqlGeneratorImpl();
			break;
		}

		// 初始化集群
		switch (enumDbConnect) {
		case DBCP:
			this.dbCluster = new DbcpDBClusterImpl(enumDb);
			break;
		default:
			this.dbCluster = new DbcpDBClusterImpl(enumDb);
			break;
		}
		// 设置集群路由器
		this.dbCluster.setDbRouter(dbRouter);
		// 设置是否生成数据库表
		this.dbCluster.setCreateTable(this.isCreateTable);
		// 设置集群数据库表生成器
		this.dbCluster.setDbGenerator(this.dbGenerator);
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
	}

	@Override
	public Number globalSave(IGlobalEntity entity) {
		CheckUtil.checkEntity(entity);

		String clusterName = entity.getClusterName();
		CheckUtil.checkClusterName(clusterName);

		return this.updater.globalSave(entity, clusterName);
	}

	@Override
	public void globalUpdate(IGlobalEntity entity) {
		CheckUtil.checkEntity(entity);

		String clusterName = entity.getClusterName();
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
	public void globalRemoveByPks(Number[] pks, Class<?> clazz, String clusterName) {
		CheckUtil.checkNumberArray(pks);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkClusterName(clusterName);

		this.updater.globalRemoveByPks(pks, clazz, clusterName);
	}

	@Override
	public Number save(IShardingEntity<?> entity) {
		CheckUtil.checkEntity(entity);

		IShardingValue<Object> shardingValue = new ShardingValue<Object>(entity.getClusterName(),
				entity.getShardingValue());
		CheckUtil.checkShardingValue(shardingValue);

		return this.updater.save(entity, shardingValue);
	}

	@Override
	public void update(IShardingEntity<?> entity) {
		CheckUtil.checkEntity(entity);

		IShardingValue<Object> shardingValue = new ShardingValue<Object>(entity.getClusterName(),
				entity.getShardingValue());
		CheckUtil.checkShardingValue(shardingValue);

		this.updater.update(entity, shardingValue);
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingValue<?> shardingValue) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingValue);

		return this.updater.saveBatch(entities, shardingValue);
	}

	@Override
	public void updateBatch(List<? extends Object> entities, IShardingValue<?> shardingValue) {
		CheckUtil.checkEntityList(entities);
		CheckUtil.checkShardingValue(shardingValue);

		this.updater.updateBatch(entities, shardingValue);
	}

	@Override
	public void removeByPk(Number pk, IShardingValue<?> shardingValue, Class<?> clazz) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		this.updater.removeByPk(pk, shardingValue, clazz);
	}

	@Override
	public void removeByPks(Number[] pks, IShardingValue<?> shardingValue, Class<?> clazz) {
		CheckUtil.checkNumberArray(pks);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		this.updater.removeByPks(pks, shardingValue, clazz);
	}

	@Override
	public <T> T findGlobalOneByQuery(IQuery query, String clusterName, Class<T> clazz) {
		return this.masterQueryer.findGlobalOneByQueryFromMaster(query, clusterName, clazz);
	}

	@Override
	public <T> T findOneByQuery(IQuery query, IShardingValue<?> shardingValue, Class<T> clazz) {
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
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberArray(pks);

		return this.masterQueryer.findGlobalByPksFromMaster(clusterName, clazz, pks);
	}

	@Override
	public <T> List<T> findGlobalByPks(List<? extends Number> pks, String clusterName, Class<T> clazz) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberList(pks);

		return this.masterQueryer.findGlobalByPksFromMaster(pks, clusterName, clazz);
	}

	@Override
	public <T> List<T> findGlobalMore(String clusterName, Class<T> clazz, int start, int limit) {
		CheckUtil.checkClusterName(clusterName);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkLimit(start, limit);

		return this.masterQueryer.findGlobalMoreFromMaster(clusterName, clazz, start, limit);
	}

	@Override
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
	public Number getCount(IShardingValue<?> shardingValue, Class<?> clazz) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.getCountFromMaster(shardingValue, clazz);
	}

	@Override
	public Number getCount(IShardingValue<?> shardingValue, SQL<?> sql) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkSQL(sql);

		return this.masterQueryer.getCountFromMaster(shardingValue, sql);
	}

	@Override
	public <T> T findByPk(Number pk, IShardingValue<?> shardingValue, Class<T> clazz) {
		CheckUtil.checkNumberGtZero(pk);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkFromMaster(pk, shardingValue, clazz);
	}

	@Override
	public <T> List<T> findByPks(IShardingValue<?> shardingValue, Class<T> clazz, Number... pks) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberArray(pks);

		return this.masterQueryer.findByPksFromMaster(shardingValue, clazz, pks);
	}

	@Override
	public <T> List<T> findByPkList(List<? extends Number> pks, IShardingValue<?> shardingValue, Class<T> clazz) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByPkListFromMaster(pks, shardingValue, clazz);
	}

	@Override
	public <T> List<T> findByShardingPair(List<IShardingValue<?>> shardingValues, Class<T> clazz, Number... pks) {
		CheckUtil.checkShardingValueList(shardingValues);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkNumberArray(pks);

		return this.masterQueryer.findByShardingPairFromMaster(shardingValues, clazz, pks);
	}

	@Override
	public <T> List<T> findByShardingPair(List<? extends Number> pks, List<IShardingValue<?>> shardingValues,
			Class<T> clazz) {
		CheckUtil.checkNumberList(pks);
		CheckUtil.checkShardingValueList(shardingValues);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByShardingPairFromMaster(pks, shardingValues, clazz);
	}

	@Override
	public <T> List<T> findMore(IShardingValue<?> shardingValue, Class<T> clazz, int start, int limit) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);
		CheckUtil.checkLimit(start, limit);

		return this.masterQueryer.findMoreFromMaster(shardingValue, clazz, start, limit);
	}

	@Override
	public <T> List<T> findBySql(SQL<T> sql, IShardingValue<?> shardingValue) {
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkSQL(sql);

		return this.masterQueryer.findBySqlFromMaster(sql, shardingValue);
	}

	@Override
	public <T> List<T> findByQuery(IQuery query, IShardingValue<?> shardingValue, Class<T> clazz) {
		CheckUtil.checkQuery(query);
		CheckUtil.checkShardingValue(shardingValue);
		CheckUtil.checkClass(clazz);

		return this.masterQueryer.findByQueryFromMaster(query, shardingValue, clazz);
	}

	@Override
	public IShardingStatistics getShardingStatistic() {
		IShardingStatistics staticstic = new ShardingStatisticsImpl();
		staticstic.setDbCluster(this.dbCluster);
		return staticstic;
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

	@Override
	public IShardingUpdate getShardingUpdate() {
		return this.updater;
	}

	@Override
	public IShardingMasterQuery getShardingMasterQuery() {
		return this.masterQueryer;
	}

	@Override
	public IShardingSlaveQuery getShardingSlaveQuery() {
		return this.slaveQueryer;
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
		try {
			this.dbCluster.shutdown();
		} catch (DBClusterException e) {
			throw new RuntimeException(e);
		}
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
