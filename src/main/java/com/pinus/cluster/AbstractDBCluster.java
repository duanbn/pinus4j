package com.pinus.cluster;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.pinus.api.IShardingKey;
import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.api.enums.EnumDBRouteAlg;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBClusterRegionInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.route.DBRouteInfo;
import com.pinus.cluster.route.IClusterRouter;
import com.pinus.cluster.route.impl.SimpleHashClusterRouterImpl;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlDBClusterConfigImpl;
import com.pinus.constant.Const;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBOperationException;
import com.pinus.exception.DBRouteException;
import com.pinus.exception.LoadConfigException;
import com.pinus.generator.IDBGenerator;
import com.pinus.generator.impl.DBMySqlGeneratorImpl;
import com.pinus.util.IOUtil;
import com.pinus.util.ReflectUtil;
import com.pinus.util.StringUtils;

/**
 * 抽象数据库集群. 主要负责初始化数据库集群的数据源对象、分表信息.<br/>
 * need to invoke startup method before use it, invoke shutdown method at last.
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
     * 数据分片信息是否从zookeeper中获取.
     */
    private boolean isShardInfoFromZk;

	/**
	 * 数据库类型.
	 */
	protected EnumDB enumDb = EnumDB.MYSQL;

	/**
	 * 路由算法. 默认使用取模哈希算法
	 */
	protected EnumDBRouteAlg enumDBRouteAlg = EnumDBRouteAlg.SIMPLE_HASH;

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
	 * 集群配置.
	 */
	private IClusterConfig config;

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
    public Collection<DBClusterInfo> getDbClusterInfo() {
        return this.dbClusterInfo.values();
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
		startup(null);
	}

	@Override
	public void startup(String xmlFilePath) throws DBClusterException {
		LOG.info("start init database cluster");

		// init db router
		switch (enumDBRouteAlg) {
		case SIMPLE_HASH:
			dbRouter = new SimpleHashClusterRouterImpl();
			break;
		default:
			dbRouter = new SimpleHashClusterRouterImpl();
			break;
		}

		// init db generator
		switch (enumDb) {
		case MYSQL:
			this.dbGenerator = new DBMySqlGeneratorImpl();
			break;
		default:
			this.dbGenerator = new DBMySqlGeneratorImpl();
			break;
		}

		try {
			config = _getConfig(xmlFilePath);
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
			List<DBTable> tables = null;
            if (isShardInfoFromZk) {
                // get table sharding info from zookeeper
				tables = getDBTableFromZk();
            } else {
                if (StringUtils.isBlank(scanPackage)) {
                    throw new DBClusterException("get shardinfo from jvm, but i can't find scanpackage full path, did you forget setScanPackage ?");
                }

                // get table sharding info from jvm
                tables = getDBTableFromJvm();
                // 表分片信息写入zookeeper
                _syncToZookeeper(tables);
            }
			if (tables.isEmpty()) {
				throw new DBClusterException("找不到可以创建库表的实体对象, package=" + scanPackage);
			}

			// 初始化表集群
			_initTableCluster(dbClusterInfo, tables);

			// 创建数据库表
			if (isCreateTable) {
				LOG.info("正在同步数据库表.");
				long start = System.currentTimeMillis();
				_createTable(tables);
				LOG.info("数据库表同步完成, 耗时:" + (System.currentTimeMillis() - start) + "ms");
			}

		} catch (Exception e) {
			throw new DBClusterException("init database cluster failure", e);
		}

		LOG.info("init database cluster done.");
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

		try {
			this.config.getZooKeeper().close();
		} catch (InterruptedException e) {
			throw new DBClusterException("关闭zookeeper连接失败", e);
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
    public List<DB> getAllMasterShardingDB(int tableNum, String clusterName, String tableName) {
        List<DB> dbs = new ArrayList<DB>();

		if (tableNum == 0) {
			throw new IllegalStateException("table number is 0");
		}

		DB db = null;
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
	public List<DB> getAllMasterShardingDB(Class<?> clazz) {
        int tableNum = ReflectUtil.getTableNum(clazz);
		if (tableNum == 0) {
			throw new IllegalStateException("table number is 0");
		}

		String clusterName = ReflectUtil.getClusterName(clazz);
		String tableName = ReflectUtil.getTableName(clazz);

        return getAllMasterShardingDB(tableNum, clusterName, tableName);
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

    @Override
    public void setShardInfoFromZk(boolean value) {
        this.isShardInfoFromZk = value;
    }

	@Override
	public List<DBTable> getDBTableFromZk() {
		List<DBTable> tables = new ArrayList<DBTable>();

		ZooKeeper zkClient = config.getZooKeeper();

		try {
			List<String> zkTableNodes = zkClient.getChildren(Const.ZK_SHARDINGINFO, false);
			byte[] tableData = null;
			for (String zkTableNode : zkTableNodes) {
				tableData = zkClient.getData(Const.ZK_SHARDINGINFO + "/" + zkTableNode, false, null);
				tables.add(IOUtil.getObject(tableData, DBTable.class));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				zkClient.close();
			} catch (InterruptedException e) {
			}
		}

		return tables;
	}

	@Override
	public List<DBTable> getDBTableFromJvm() {
		try {
			return this.dbGenerator.scanEntity(this.scanPackage);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 将表分片信息同步到zookeeper.
	 */
	private void _syncToZookeeper(List<DBTable> tables) throws Exception {
		ZooKeeper zkClient = config.getZooKeeper();
		try {
			Stat stat = zkClient.exists(Const.ZK_SHARDINGINFO, false);
			if (stat == null) {
				// 创建根节点
				zkClient.create(Const.ZK_SHARDINGINFO, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			byte[] tableData = null;
			String tableName = null;
			for (DBTable table : tables) {
				tableData = IOUtil.getBytes(table);
				tableName = table.getName();

				String zkTableNode = Const.ZK_SHARDINGINFO + "/" + tableName;
				stat = zkClient.exists(zkTableNode, false);
				if (stat == null) {
					zkClient.create(zkTableNode, tableData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} else {
					zkClient.setData(zkTableNode, tableData, -1);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (zkClient != null) {
				zkClient.close();
			}
		}
		LOG.info("sharding info of tables have flushed to zookeeper done.");
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

	@Override
	public IClusterRouter getDbRouter() {
		return dbRouter;
	}

	@Override
	public void setDbRouteAlg(EnumDBRouteAlg routeAlg) {
		this.enumDBRouteAlg = routeAlg;
	}

	@Override
	public EnumDBRouteAlg getDbRouteAlg() {
		return this.enumDBRouteAlg;
	}

	/**
	 * 读取配置.
	 * 
	 * @return 配置信息.
	 */
	private IClusterConfig _getConfig(String xmlFilePath) throws LoadConfigException {
		IClusterConfig config = null;

		if (StringUtils.isBlank(xmlFilePath)) {
			config = XmlDBClusterConfigImpl.getInstance();
		} else {
			config = XmlDBClusterConfigImpl.getInstance(xmlFilePath);
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

	@Override
	public void setCreateTable(boolean isCreateTable) {
		this.isCreateTable = isCreateTable;
	}

	@Override
	public boolean isCreateTable() {
		return this.isCreateTable;
	}

	@Override
	public IDBGenerator getDbGenerator() {
		return this.dbGenerator;
	}

	@Override
	public void setScanPackage(String scanPackage) {
		this.scanPackage = scanPackage;
	}

	@Override
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster() {
		return this.tableCluster;
	}

	@Override
	public IClusterConfig getClusterConfig() {
		return this.config;
	}

}
