package com.pinus.cluster;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.pinus.api.IShardingKey;
import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBClusterRegionInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.route.DBRouteInfo;
import com.pinus.cluster.route.IClusterRouter;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlDBClusterConfigImpl;
import com.pinus.constant.Const;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBOperationException;
import com.pinus.exception.DBRouteException;
import com.pinus.exception.LoadConfigException;
import com.pinus.generator.IDBGenerator;
import com.pinus.util.ReflectUtil;
import com.pinus.util.StringUtils;

/**
 * 抽象数据库集群. 主要负责初始化数据库集群的数据源对象、分表信息.
 * 
 * @author duanbn
 */
public abstract class AbstractDBCluster implements IDBCluster {

	/**
	 * 日志
	 */
	private static final Logger LOG = Logger.getLogger(AbstractDBCluster.class);

	/**
	 * 是否创建数据库表.
	 */
	private boolean isCreateTable;

	/**
	 * 扫描数据对象包.
	 */
	private String scanPackage;

	/**
	 * 数据库类型.
	 */
	protected EnumDB enumDb;

	/**
	 * 数据库表生成器.
	 */
	private IDBGenerator dbGenerator;

	/**
	 * 数据库路由器
	 */
	private IClusterRouter dbRouter;

	/**
	 * 分库分表信息. {clusterName, clusterInfo}
	 */
	private Map<String, DBClusterInfo> dbClusterInfo;

	/**
	 * 集群中的表集合. {集群名称, {分库下标, {表名, 分表数}}}
	 */
	Map<String, Map<Integer, Map<String, Integer>>> tableCluster = new HashMap<String, Map<Integer, Map<String, Integer>>>();

	/**
	 * 构造方法.
	 * 
	 * @param enumDb
	 *            数据库类型.
	 */
	public AbstractDBCluster(EnumDB enumDb) {
		this.enumDb = enumDb;
	}

	@Override
	public DBClusterInfo getDbClusterInfo(String clusterName) {
		DBClusterInfo clusterInfo = dbClusterInfo.get(clusterName);

		if (clusterInfo == null) {
			throw new DBOperationException("找不到集群信息, clusterName=" + clusterName);
		}

		return clusterInfo;
	}

	@Override
	public void startup() throws DBClusterException {
		LOG.info("开始初始化数据库集群.");

		// 加载数据
		IClusterConfig config;
		try {
			config = _getConfig();
		} catch (LoadConfigException e) {
			throw new RuntimeException(e);
		}

		// 加载DB集群信息
		dbClusterInfo = config.getDBClusterInfo();

		// 给路由器设置集群信息
		if (this.dbRouter == null) {
			throw new DBClusterException("启动前需要设置DBClusterRouter");
		}
		// 设置集群信息
		this.dbRouter.setDbClusterInfo(dbClusterInfo);
		// 设置hash算法
		this.dbRouter.setHashAlgo(config.getHashAlgo());

		try {
			// 初始化主集群连接
			_initDBCluster(this.dbClusterInfo);

			// 初始化数据表集群信息.
			// 优先使用shard_cluster表的信息，获取失败则使用@Table的信息
			if (scanPackage == null || scanPackage.equals("")) {
				throw new IllegalStateException("未设置需要扫描的实体对象包, 参考IShardingStorageClient.setScanPackage()");
			}
			List<DBTable> tables = this.dbGenerator.scanEntity(scanPackage);
			if (tables.isEmpty()) {
				throw new DBClusterException("找不到可以创建库表的实体对象, package=" + scanPackage);
			}
			_initTableCluster(dbClusterInfo, tables);

			// 创建数据库表
			if (isCreateTable) {
				LOG.info("正在同步数据库表.");
				long start = System.currentTimeMillis();
				_createTable(tables);
				LOG.info("数据库表同步完成, 耗时:" + (System.currentTimeMillis() - start) + "ms");
			}

		} catch (Exception e) {
			throw new DBClusterException("初始化数据库集群失败", e);
		}

		LOG.info("初始化数据库集群完毕.");
	}

	@Override
	public void shutdown() throws DBClusterException {
		try {

			for (Map.Entry<String, DBClusterInfo> entry : this.dbClusterInfo.entrySet()) {
				// 关闭全局库
				// 主全局库
				DBConnectionInfo masterGlobal = entry.getValue().getMasterGlobalConnection();
				if (masterGlobal != null)
					closeDataSource(masterGlobal);

				// 从全局库
				List<DBConnectionInfo> slaveDbs = entry.getValue().getSlaveGlobalConnection();
				if (slaveDbs != null && !slaveDbs.isEmpty()) {
					for (DBConnectionInfo slaveGlobal : slaveDbs) {
						closeDataSource(slaveGlobal);
					}
				}

				// 关闭集群库
				for (DBClusterRegionInfo regionInfo : entry.getValue().getDbRegions()) {
					// 主集群
					for (DBConnectionInfo dbConnInfo : regionInfo.getMasterConnection()) {
						closeDataSource(dbConnInfo);
					}

					// 从集群
					for (List<DBConnectionInfo> dbConnInfos : regionInfo.getSlaveConnection()) {
						for (DBConnectionInfo dbConnInfo : dbConnInfos) {
							closeDataSource(dbConnInfo);
						}
					}
				}
			}

		} catch (Exception e) {
			throw new DBClusterException("关闭数据库集群失败", e);
		}
	}

	@Override
	public DBConnectionInfo getMasterGlobalConn(String clusterName) throws DBClusterException {
		DBClusterInfo dbClusterInfo = this.dbClusterInfo.get(clusterName);
		if (dbClusterInfo == null) {
			throw new DBClusterException("没有找到集群信息, clustername=" + clusterName);
		}

		DBConnectionInfo masterConnection = dbClusterInfo.getMasterGlobalConnection();
		if (masterConnection == null) {
			throw new DBClusterException("此集群没有配置全局主库, clustername=" + clusterName);
		}
		return masterConnection;
	}

	@Override
	public DBConnectionInfo getSlaveGlobalDbConn(String clusterName, EnumDBMasterSlave slave) throws DBClusterException {
		DBClusterInfo dbClusterInfo = this.dbClusterInfo.get(clusterName);
		if (dbClusterInfo == null) {
			throw new DBClusterException("没有找到集群信息, clustername=" + clusterName);
		}

		List<DBConnectionInfo> slaveDbs = dbClusterInfo.getSlaveGlobalConnection();
		if (slaveDbs == null || slaveDbs.isEmpty()) {
			throw new DBClusterException("此集群没有配置全局从库, clustername=" + clusterName);
		}
		DBConnectionInfo slaveConnection = slaveDbs.get(slave.getValue());
		return slaveConnection;
	}

	@Override
	public DB selectDbFromMaster(String tableName, IShardingKey<?> value) throws DBClusterException {

		// 计算分库
		// 计算路由信息
		DBRouteInfo routeInfo = null;
		try {
			routeInfo = dbRouter.select(EnumDBMasterSlave.MASTER, tableName, value);
		} catch (DBRouteException e) {
			throw new DBClusterException(e);
		}
		String clusterName = routeInfo.getClusterName();
		int dbIndex = routeInfo.getDbIndex();
		int tableIndex = routeInfo.getTableIndex();

		// 获取连接信息
		DBClusterInfo dbClusterInfo = this.dbClusterInfo.get(clusterName);
		if (dbClusterInfo == null) {
			throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName);
		}
		DBClusterRegionInfo regionInfo = dbClusterInfo.getDbRegions().get(routeInfo.getRegionIndex());
		if (regionInfo == null) {
			throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName);
		}
		List<DBConnectionInfo> masterConntions = regionInfo.getMasterConnection();
		if (masterConntions == null || masterConntions.isEmpty()) {
			throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName);
		}

		DataSource datasource = masterConntions.get(dbIndex).getDatasource();

		// 返回分库分表信息
		DB db = new DB();
		db.setDatasource(datasource);
		db.setTableName(tableName);
		db.setTableIndex(tableIndex);
		db.setClusterName(clusterName);
		db.setDbIndex(dbIndex);
		db.setDbCluster(this);
		db.setStart(regionInfo.getStart());
		db.setEnd(regionInfo.getEnd());

		return db;
	}

	@Override
	public DB selectDbFromSlave(EnumDBMasterSlave slaveNum, String tableName, IShardingKey<?> value)
			throws DBClusterException {

		// 计算分库
		// 计算路由信息
		DBRouteInfo routeInfo = null;
		try {
			routeInfo = dbRouter.select(slaveNum, tableName, value);
		} catch (DBRouteException e) {
			throw new DBClusterException(e);
		}
		// 获取分库分表的下标
		String clusterName = routeInfo.getClusterName();
		int dbIndex = routeInfo.getDbIndex();
		int tableIndex = routeInfo.getTableIndex();

		// 获取连接信息
		DBClusterInfo dbClusterInfo = this.dbClusterInfo.get(clusterName);
		if (dbClusterInfo == null) {
			throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName + ", slavenum="
					+ slaveNum.getValue());
		}
		DBClusterRegionInfo regionInfo = dbClusterInfo.getDbRegions().get(routeInfo.getRegionIndex());
		if (regionInfo == null) {
			throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName + ", slavenum="
					+ slaveNum.getValue());
		}
		List<DBConnectionInfo> slaveConnections = regionInfo.getSlaveConnection().get(slaveNum.getValue());
		if (slaveConnections == null || slaveConnections.isEmpty()) {
			throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName + ", slavenum="
					+ slaveNum.getValue());
		}

		DataSource datasource = slaveConnections.get(dbIndex).getDatasource();

		// 返回分库分表信息
		DB db = new DB();
		db.setDatasource(datasource);
		db.setClusterName(clusterName);
		db.setDbIndex(dbIndex);
		db.setTableName(tableName);
		db.setTableIndex(tableIndex);
		db.setDbCluster(this);
		db.setStart(regionInfo.getStart());
		db.setEnd(regionInfo.getEnd());

		return db;
	}

	@Override
	public List<DB> getAllMasterShardingDB(Class<?> clazz) {
		List<DB> dbs = new ArrayList<DB>();

		int tableNum = ReflectUtil.getTableNum(clazz);
		if (tableNum == 0) {
			throw new IllegalStateException("table number is 0");
		}

		DB db = null;
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);
		DBClusterInfo dbClusterInfo = this.getDbClusterInfo(clusterName);
		for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
			int dbIndex = 0;
			for (DBConnectionInfo connInfo : region.getMasterConnection()) {
				for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
					db = new DB();
					db.setClusterName(clusterName);
					db.setDbCluster(this);
					db.setDatasource(connInfo.getDatasource());
					db.setDbIndex(dbIndex);
					db.setEnd(region.getEnd());
					db.setStart(region.getStart());
					db.setTableName(tableName);
					db.setTableIndex(tableIndex);
					dbs.add(db);
				}
				dbIndex++;
			}
		}

		return dbs;
	}

	@Override
	public List<DB> getAllSlaveShardingDB(Class<?> clazz, EnumDBMasterSlave slave) {
		List<DB> dbs = new ArrayList<DB>();

		int tableNum = ReflectUtil.getTableNum(clazz);
		if (tableNum == 0) {
			throw new IllegalStateException("table number is 0");
		}

		DB db = null;
		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);
		DBClusterInfo dbClusterInfo = this.getDbClusterInfo(clusterName);
		for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
			int dbIndex = 0;
			for (DBConnectionInfo connInfo : region.getSlaveConnection().get(slave.getValue())) {
				for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
					db = new DB();
					db.setClusterName(clusterName);
					db.setDbCluster(this);
					db.setDatasource(connInfo.getDatasource());
					db.setDbIndex(dbIndex);
					db.setEnd(region.getEnd());
					db.setStart(region.getStart());
					db.setTableName(tableName);
					db.setTableIndex(tableIndex);
					dbs.add(db);
				}
				dbIndex++;
			}
		}

		return dbs;
	}

	/**
	 * 创建数据库表.
	 * 
	 * @throws
	 * @throws IOException
	 */
	private void _createTable(List<DBTable> tables) throws Exception {
		// {集群名称, {分库下标, {表名, 分表数}}}
		Map<String, Map<Integer, Map<String, Integer>>> tableCluster = this.dbRouter.getTableCluster();

		String clusterName = null;
		Map<Integer, Map<String, Integer>> oneDbTables = null;
		for (DBTable table : tables) {
			clusterName = table.getCluster();
			if (table.getShardingNum() > 0) { // 当ShardingNumber大于0时表示分库分表
				// 读取分表信息
				oneDbTables = tableCluster.get(clusterName);
				DBClusterInfo dbClusterInfo = this.dbClusterInfo.get(clusterName);
				if (oneDbTables == null || dbClusterInfo == null) {
					throw new DBClusterException("找不到相关的集群信息, clusterName=" + clusterName);
				}

				for (Integer dbIndex : oneDbTables.keySet()) {

					// 创建主库库表
					for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
						Connection dbConn = region.getMasterConnection().get(dbIndex).getDatasource().getConnection();
						int tableNum = oneDbTables.get(dbIndex).get(table.getName());
						this.dbGenerator.syncTable(dbConn, table, tableNum);
						dbConn.close();
					}

					// 创建从库库表
					for (DBClusterRegionInfo region : dbClusterInfo.getDbRegions()) {
						List<List<DBConnectionInfo>> slaveDbs = region.getSlaveConnection();
						for (List<DBConnectionInfo> slaveConns : slaveDbs) {
							for (DBConnectionInfo dbConnInfo : slaveConns) {
								Connection dbConn = dbConnInfo.getDatasource().getConnection();
								int tableNum = oneDbTables.get(dbIndex).get(table.getName());
								this.dbGenerator.syncTable(dbConn, table, tableNum);
								dbConn.close();
							}
						}
					}
				}

			} else { // 当ShardingNumber等于0时表示全局表

				for (Map.Entry<String, DBClusterInfo> entry : this.dbClusterInfo.entrySet()) {
					// 全局主库
					DBConnectionInfo dbConnInfo = entry.getValue().getMasterGlobalConnection();
					if (dbConnInfo != null) {
						DataSource globalDs = dbConnInfo.getDatasource();
						if (globalDs != null) {
							Connection conn = globalDs.getConnection();
							this.dbGenerator.syncTable(conn, table);
							conn.close();
						}
					}

					// 全局从库
					List<DBConnectionInfo> slaveDbs = entry.getValue().getSlaveGlobalConnection();
					if (slaveDbs != null && !slaveDbs.isEmpty()) {
						for (DBConnectionInfo slaveConnInfo : slaveDbs) {
							Connection conn = slaveConnInfo.getDatasource().getConnection();
							this.dbGenerator.syncTable(conn, table);
							conn.close();
						}
					}
				}

			}

		}
	}

	private void _initDBCluster(Map<String, DBClusterInfo> dbClusterInfo) {
		for (Map.Entry<String, DBClusterInfo> entry : dbClusterInfo.entrySet()) {
			// 初始化全局主库
			DBConnectionInfo masterGlobalConnection = entry.getValue().getMasterGlobalConnection();
			if (masterGlobalConnection != null)
				buildDataSource(masterGlobalConnection);

			// 初始化全局从库
			List<DBConnectionInfo> slaveDbs = entry.getValue().getSlaveGlobalConnection();
			if (slaveDbs != null && !slaveDbs.isEmpty()) {
				for (DBConnectionInfo slaveGlobalConnection : slaveDbs) {
					buildDataSource(slaveGlobalConnection);
				}
			}

			// 初始化集群
			for (DBClusterRegionInfo regionInfo : entry.getValue().getDbRegions()) {
				// 初始化集群主库
				for (DBConnectionInfo masterConnection : regionInfo.getMasterConnection()) {
					buildDataSource(masterConnection);
				}

				// 初始化集群从库
				for (List<DBConnectionInfo> slaveConnections : regionInfo.getSlaveConnection()) {
					for (DBConnectionInfo slaveConnection : slaveConnections) {
						buildDataSource(slaveConnection);
					}
				}
			}

		}
	}

	/**
	 * 初始化分表. 并将分表信息设置到分库分表路由器, 主库和从库的分表信息是相同的，因此使用主库集群进行设置.
	 * 
	 * @throws DBClusterException
	 *             初始化失败
	 */
	private void _initTableCluster(Map<String, DBClusterInfo> dbCluster, List<DBTable> tables)
			throws DBClusterException {

		// {分库下标, {表名, 分表数}}
		Map<Integer, Map<String, Integer>> oneDbTable = null;

		Map<String, Integer> tableNum = null;
		for (Map.Entry<String, DBClusterInfo> entry : dbClusterInfo.entrySet()) {
			oneDbTable = new HashMap<Integer, Map<String, Integer>>();

			String clusterName = entry.getKey();

			int dbNum = entry.getValue().getDbRegions().get(0).getMasterConnection().size();

			for (int i = 0; i < dbNum; i++) {
				tableNum = _loadTableCluster(clusterName, tables);
				oneDbTable.put(i, tableNum);
			}

			tableCluster.put(clusterName, oneDbTable);
		}
		this.dbRouter.setTableCluster(tableCluster);
	}

	/**
	 * 加载数据表集群信息. 基于@Table注解读取信息.
	 */
	private Map<String, Integer> _loadTableCluster(String clusterName, List<DBTable> tables) throws DBClusterException {
		Map<String, Integer> tableNum = new HashMap<String, Integer>();
		for (DBTable table : tables) {
			if (table.getCluster().equals(clusterName)) {
				String tableName = table.getName();
				int shardingNum = table.getShardingNum();
				tableNum.put(tableName, shardingNum);
			}
		}

		if (tableNum.isEmpty()) {
			throw new DBClusterException("找不到可以创建库表的实体对象, 集群名=" + clusterName);
		}

		return tableNum;
	}

	public IClusterRouter getDbRouter() {
		return dbRouter;
	}

	public void setDbRouter(IClusterRouter dbRouter) {
		this.dbRouter = dbRouter;
	}

	/**
	 * 读取配置. 三种配置信息获取方式，1. 从classpath根路径的storage-config.properties中获取。 2.
	 * 从zookeeper中获取. 优先从zookeeper中加载，其次从指定的文件，默认从classpath根路径
	 * 
	 * @return 配置信息.
	 */
	private IClusterConfig _getConfig() throws LoadConfigException {
		IClusterConfig config = null;

		String zkHost = System.getProperty(Const.SYSTEM_PROPERTY_ZKHOST);

		if (StringUtils.isNotBlank(zkHost)) {
			// TODO: 配置信息从zookeeper中获取.
		} else {
			config = XmlDBClusterConfigImpl.getInstance();
		}

		return config;
	}

	/**
	 * 创建数据源连接.
	 */
	public abstract void buildDataSource(DBConnectionInfo dbConnInfo);

	/**
	 * 关闭数据源连接
	 * 
	 * @param dbConnInfo
	 */
	public abstract void closeDataSource(DBConnectionInfo dbConnInfo);

	public IDBGenerator getDbGenerator() {
		return dbGenerator;
	}

	public void setDbGenerator(IDBGenerator dbGenerator) {
		this.dbGenerator = dbGenerator;
	}

	@Override
	public void setCreateTable(boolean isCreateTable) {
		this.isCreateTable = isCreateTable;
	}

	@Override
	public boolean isCreateTable() {
		return this.isCreateTable;
	}

	public String getScanPackage() {
		return scanPackage;
	}

	@Override
	public void setScanPackage(String scanPackage) {
		this.scanPackage = scanPackage;
	}

	@Override
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster() {
		return this.tableCluster;
	}

}
