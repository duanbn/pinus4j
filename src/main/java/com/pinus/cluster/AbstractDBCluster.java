package com.pinus.cluster;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import com.pinus.api.IShardingValue;
import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.beans.DBTableColumn;
import com.pinus.cluster.beans.DataTypeBind;
import com.pinus.cluster.route.DBRouteInfo;
import com.pinus.cluster.route.IClusterRouter;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlClusterConfigImpl;
import com.pinus.constant.Const;
import com.pinus.exception.DBClusterException;
import com.pinus.exception.DBRouteException;
import com.pinus.exception.LoadConfigException;
import com.pinus.generator.IDBGenerator;
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
	 * 主全局库.
	 */
	private Map<String, DataSource> masterGlobalDs = new HashMap<String, DataSource>();

	/**
	 * 从全局库.
	 */
	private Map<String, Map<Integer, DataSource>> slaveGlobalDs = new HashMap<String, Map<Integer, DataSource>>();

	/**
	 * 主库集群数据源. {集群名, [集群连接]}
	 */
	private Map<String, List<DataSource>> masterDSCluster = new HashMap<String, List<DataSource>>();

	/**
	 * 从库集群数据源. {集群名, {从库号, [集群连接]}}
	 */
	private Map<String, Map<Integer, List<DataSource>>> slaveDSCluster = new HashMap<String, Map<Integer, List<DataSource>>>();

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
	public void startup() throws DBClusterException {
		LOG.info("开始初始化数据库集群.");

		// 加载数据
		IClusterConfig config;
		try {
			config = _getConfig();
		} catch (LoadConfigException e) {
			throw new RuntimeException(e);
		}

		// 加载主全局库
		Map<String, DBConnectionInfo> masterGlobalInfo = config.loadMasterGlobalInfo();
		// 加载从全局库
		Map<String, Map<Integer, DBConnectionInfo>> slaveGlobalInfo = config.loadSlaveGlobalInfo();
		// 加载主库集群
		Map<String, DBClusterInfo> masterDbCluster = config.loadMasterDbClusterInfo();
		// 加载从库集群
		Map<String, Map<Integer, DBClusterInfo>> slaveDbCluster = config.loadSlaveDbClusterInfo();

		// 给路由器设置集群信息
		if (this.dbRouter == null) {
			throw new DBClusterException("启动前需要设置DBClusterRouter");
		}
		// 设置主库信息
		this.dbRouter.setMasterDbClusterInfo(masterDbCluster);
		// 设置从库信息
		this.dbRouter.setSlaveDbClusterInfo(slaveDbCluster);
		// 设置hash算法
		this.dbRouter.setHashAlgo(config.getHashAlgo());

		try {
			// 初始化主全局库数据源
			for (Map.Entry<String, DBConnectionInfo> entry : masterGlobalInfo.entrySet()) {
				masterGlobalDs.put(entry.getKey(), buildDataSource(entry.getValue()));
			}
			// 初始化从全局库数据源
			for (Map.Entry<String, Map<Integer, DBConnectionInfo>> entry : slaveGlobalInfo.entrySet()) {
				String clusterName = entry.getKey();
				Map<Integer, DataSource> oneSlave = new HashMap<Integer, DataSource>();
				for (Map.Entry<Integer, DBConnectionInfo> entry1 : entry.getValue().entrySet()) {
					oneSlave.put(entry1.getKey(), buildDataSource(entry1.getValue()));
				}
				slaveGlobalDs.put(clusterName, oneSlave);
			}

			// 初始化主库集群数据源
			_initMasterClusterDs(masterDbCluster);

			// 初始化从库集群数据源
			_initSlaveClusterDs(slaveDbCluster);

			// 初始化数据表集群信息.
			// 优先使用shard_cluster表的信息，获取失败则使用@Table的信息
			if (scanPackage == null || scanPackage.equals("")) {
				throw new IllegalStateException("未设置需要扫描的实体对象包, 参考IShardingStorageClient.setScanPackage()");
			}
			List<DBTable> tables = this.dbGenerator.scanEntity(scanPackage);
			_initTableCluster(masterDbCluster, tables);

			// 创建数据库表
			if (isCreateTable) {
				LOG.info("正在同步数据库表.");
				long start = System.currentTimeMillis();
				_createGlobalIdTable();
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
			// 关闭主全局库资源
			for (Map.Entry<String, DataSource> entry : masterGlobalDs.entrySet()) {
				((BasicDataSource) entry.getValue()).close();
			}

			// 关闭从全局库资源
			for (Map.Entry<String, Map<Integer, DataSource>> entry : slaveGlobalDs.entrySet()) {
				for (Map.Entry<Integer, DataSource> entry1 : entry.getValue().entrySet()) {
					((BasicDataSource) entry1.getValue()).close();
				}
			}

			// 关闭主库集群数据源
			for (Map.Entry<String, List<DataSource>> entry : masterDSCluster.entrySet()) {
				for (DataSource ds : entry.getValue()) {
					((BasicDataSource) ds).close();
				}
			}

			// 关闭从库集群数据源
			for (Map.Entry<String, Map<Integer, List<DataSource>>> slaveCluster : slaveDSCluster.entrySet()) {
				for (Map.Entry<Integer, List<DataSource>> slaveList : slaveCluster.getValue().entrySet()) {
					for (DataSource ds : slaveList.getValue()) {
						((BasicDataSource) ds).close();
					}
				}
			}
		} catch (Exception e) {
			throw new DBClusterException("关闭数据库集群失败", e);
		}
	}

	@Override
	public Connection getMasterGlobalDbConn(String clusterName) throws SQLException {
		DataSource ds = this.masterGlobalDs.get(clusterName);

		if (ds == null) {
			throw new IllegalArgumentException("找不到主全局库, clusterName=" + clusterName);
		}

		return ds.getConnection();
	}

	@Override
	public Connection getSlaveGlobalDbConn(String clusterName, EnumDBMasterSlave slave) throws SQLException {
		Map<Integer, DataSource> oneSlave = this.slaveGlobalDs.get(clusterName);
		if (oneSlave == null) {
			throw new IllegalArgumentException("找不到从全局库, clusterName=" + clusterName + ", slave num="
					+ slave.getValue());
		}
		DataSource ds = oneSlave.get(slave.getValue());

		return ds.getConnection();
	}

	@Override
	public DB getGlobalIdFromMaster(String clusterName) throws DBClusterException {
		if (StringUtils.isBlank(clusterName)) {
			throw new IllegalArgumentException("参数错误, 集群名不能为空");
		}

		DataSource globalDs = this.masterGlobalDs.get(clusterName);
		Connection dbConn = null;
		try {
			dbConn = globalDs.getConnection();
		} catch (SQLException e) {
			throw new DBClusterException(e);
		}

		DB db = new DB();
		db.setDbConn(dbConn);
		db.setTableName(Const.TABLE_GLOBALID_NAME);
		db.setDbCluster(this);
		db.setClusterName(clusterName);
		db.setDbIndex(0);

		return db;
	}

	@Override
	public DB selectDbFromMaster(String tableName, IShardingValue<?> value) throws DBClusterException {

		// 计算分库
		DBRouteInfo routeInfo = null;
		try {
			routeInfo = dbRouter.select(EnumDBMasterSlave.MASTER, tableName, value);
		} catch (DBRouteException e) {
			throw new DBClusterException(e);
		}
		String clusterName = routeInfo.getClusterName();
		int dbIndex = routeInfo.getDbIndex();
		int tableIndex = routeInfo.getTableIndex();

		List<DataSource> dsList = masterDSCluster.get(clusterName);
		if (dsList == null || dsList.isEmpty()) {
			throw new DBClusterException("找不到数据库集群, db name=" + clusterName);
		}

		Connection dbConn = null;
		try {
			dbConn = dsList.get(dbIndex).getConnection();
		} catch (SQLException e) {
			throw new DBClusterException(e);
		}

		DB db = new DB();
		db.setDbConn(dbConn);
		db.setTableName(tableName);
		db.setTableIndex(tableIndex);
		db.setClusterName(clusterName);
		db.setDbIndex(dbIndex);
		db.setDbCluster(this);

		return db;
	}

	@Override
	public DB selectDbFromSlave(EnumDBMasterSlave slaveNum, String tableName, IShardingValue<?> value)
			throws DBClusterException {

		// 计算分库
		DBRouteInfo routeInfo = null;
		try {
			routeInfo = dbRouter.select(slaveNum, tableName, value);
		} catch (DBRouteException e) {
			throw new DBClusterException(e);
		}
		String clusterName = routeInfo.getClusterName();
		int dbIndex = routeInfo.getDbIndex();
		int tableIndex = routeInfo.getTableIndex();

		Map<Integer, List<DataSource>> slaveCluster = slaveDSCluster.get(clusterName);
		List<DataSource> dsList = slaveCluster.get(slaveNum.getValue());
		if (dsList == null || dsList.isEmpty()) {
			throw new DBClusterException("找不到数据库集群, db name=" + clusterName);
		}
		Connection dbConn = null;
		try {
			dbConn = dsList.get(dbIndex).getConnection();
		} catch (SQLException e) {
			throw new DBClusterException(e);
		}

		DB db = new DB();
		db.setDbConn(dbConn);
		db.setClusterName(clusterName);
		db.setDbIndex(dbIndex);
		db.setTableName(tableName);
		db.setTableIndex(tableIndex);
		db.setDbCluster(this);

		return db;
	}

	/**
	 * 创建生成全局唯一id表.
	 * 
	 * @throws Exception
	 */
	private void _createGlobalIdTable() throws Exception {
		// 创建主global_id表
		DBTable table = new DBTable(Const.TABLE_GLOBALID_NAME);
		DBTableColumn column = new DBTableColumn();
		column.setAutoIncrement(false);
		column.setCanNull(false);
		column.setField(Const.TABLE_GLOBALID_FIELD_TABLENAME);
		column.setLength(30);
		column.setPrimaryKey(true);
		column.setType(DataTypeBind.STRING.getDBType());
		table.addColumn(column);

		column = new DBTableColumn();
		column.setAutoIncrement(false);
		column.setCanNull(false);
		column.setField(Const.TABLE_GLOBALID_FIELD_ID);
		column.setLength(20);
		column.setPrimaryKey(false);
		column.setType(DataTypeBind.LONG.getDBType());
		table.addColumn(column);

		for (Map.Entry<String, DataSource> entry : this.masterGlobalDs.entrySet()) {
			Connection conn = entry.getValue().getConnection();
			this.dbGenerator.syncTable(conn, table);
			conn.close();
		}

		for (Map.Entry<String, Map<Integer, DataSource>> entry : this.slaveGlobalDs.entrySet()) {
			for (Map.Entry<Integer, DataSource> entry1 : entry.getValue().entrySet()) {
				Connection conn = entry1.getValue().getConnection();
				this.dbGenerator.syncTable(conn, table);
				conn.close();
			}
		}
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

				// 创建主库库表
				for (Integer dbIndex : oneDbTables.keySet()) {
					Connection dbConn = masterDSCluster.get(clusterName).get(dbIndex).getConnection();
					LOG.debug("开始创建主库库表, 库名:" + dbConn.getCatalog());
					long start = System.currentTimeMillis();
					int tableNum = oneDbTables.get(dbIndex).get(table.getName());
					this.dbGenerator.syncTable(dbConn, table, tableNum);
					dbConn.close();
					LOG.debug("创建完毕， 耗时" + (System.currentTimeMillis() - start) + "ms");
				}

				// 创建从库库表
				Map<Integer, List<DataSource>> slaveDbs = slaveDSCluster.get(clusterName);
				if (slaveDbs == null) {
					continue;
				}
				for (Map.Entry<Integer, List<DataSource>> entry : slaveDbs.entrySet()) {
					for (Integer dbIndex : oneDbTables.keySet()) {
						Connection dbConn = entry.getValue().get(dbIndex).getConnection();
						LOG.debug("开始创建从库库表, 库名:" + dbConn.getCatalog() + ", 从库号:" + entry.getKey());
						long start = System.currentTimeMillis();
						int tableNum = oneDbTables.get(dbIndex).get(table.getName());
						this.dbGenerator.syncTable(dbConn, table, tableNum);
						dbConn.close();
						LOG.debug("创建完毕， 耗时" + (System.currentTimeMillis() - start) + "ms");
					}
				}
			} else { // 当ShardingNumber等于0时表示全局表
				// 全局主库
				DataSource globalDs = this.masterGlobalDs.get(clusterName);
				if (globalDs != null) {
					Connection conn = globalDs.getConnection();
					this.dbGenerator.syncTable(conn, table);
					conn.close();
				}
				// 全局从库
				Map<Integer, DataSource> slaveDs = this.slaveGlobalDs.get(clusterName);
				if (slaveDs != null) {
					for (DataSource ds : slaveDs.values()) {
						Connection conn = ds.getConnection();
						this.dbGenerator.syncTable(conn, table);
						conn.close();
					}
				}
			}

		}
	}

	/**
	 * 初始化主库集群.
	 */
	private void _initMasterClusterDs(Map<String, DBClusterInfo> masterDbCluster) {
		for (Map.Entry<String, DBClusterInfo> clusterInfo : masterDbCluster.entrySet()) {
			String clusterName = clusterInfo.getKey();
			DBClusterInfo dbClusterInfo = clusterInfo.getValue();

			List<DataSource> dsList = new ArrayList<DataSource>();
			for (DBConnectionInfo dbConnInfo : dbClusterInfo.getDbConnInfos()) {
				dsList.add(buildDataSource(dbConnInfo));
			}

			masterDSCluster.put(clusterName, dsList);
		}
	}

	/**
	 * 初始化从库集群.
	 */
	private void _initSlaveClusterDs(Map<String, Map<Integer, DBClusterInfo>> slaveDbCluster) {
		for (Map.Entry<String, Map<Integer, DBClusterInfo>> clusterInfo : slaveDbCluster.entrySet()) {

			String clusterName = clusterInfo.getKey();
			Map<Integer, DBClusterInfo> slaveDbClusters = clusterInfo.getValue();

			Map<Integer, List<DataSource>> slaveDs = new HashMap<Integer, List<DataSource>>();
			for (Map.Entry<Integer, DBClusterInfo> entry : slaveDbClusters.entrySet()) {

				List<DataSource> dsList = new ArrayList<DataSource>();
				for (DBConnectionInfo dbConnInfo : entry.getValue().getDbConnInfos()) {
					dsList.add(buildDataSource(dbConnInfo));
				}

				slaveDs.put(entry.getKey(), dsList);
			}

			this.slaveDSCluster.put(clusterName, slaveDs);

		}
	}

	/**
	 * 初始化分表. 并将分表信息设置到分库分表路由器, 主库和从库的分表信息是相同的，因此使用主库集群进行设置.
	 * 
	 * @throws DBClusterException
	 *             初始化失败
	 */
	private void _initTableCluster(Map<String, DBClusterInfo> masterDbCluster, List<DBTable> tables)
			throws DBClusterException {

		// {分库下标, {表名, 分表数}}
		Map<Integer, Map<String, Integer>> oneDbTable = null;

		Map<String, Integer> tableNum = null;
		for (Map.Entry<String, DBClusterInfo> clusterInfo : masterDbCluster.entrySet()) {
			oneDbTable = new HashMap<Integer, Map<String, Integer>>();
			String clusterName = clusterInfo.getKey();
			int dbNum = clusterInfo.getValue().getDbConnInfos().size();
			for (int i = 0; i < dbNum; i++) {
				try {
					tableNum = _loadTableCluster(clusterName, i);
				} catch (DBClusterException e) {
					tableNum = _loadTableCluster(clusterName, tables);
				}
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

	/**
	 * 加载数据表集群信息. 从零库的shard_cluster表读取信息.
	 * 
	 * @param clusterName
	 *            集群名
	 * @param dbIndex
	 *            分库下标
	 * 
	 * @return {表名, 分表下标}
	 */
	private Map<String, Integer> _loadTableCluster(String clusterName, int dbIndex) throws DBClusterException {
		DataSource zeroDs = _getZero(clusterName);

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = zeroDs.getConnection();

			ps = conn.prepareStatement(Const.SQL_SELECT_SHARDCLUSTER);
			ps.setString(1, clusterName);
			ps.setInt(2, dbIndex);

			Map<String, Integer> tableNum = new HashMap<String, Integer>();
			rs = ps.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString(Const.FIELD_TABLE_NAME);
				if (StringUtils.isBlank(tableName)) {
					throw new DBClusterException("分表配置错误, 分表明不能为空");
				}

				int num = rs.getInt(Const.FIELD_TABLE_NUM);
				if (num <= 0) {
					throw new DBClusterException("分表配置错误, 分表的数量小于0");
				}
				tableNum.put(tableName, num);
			}
			return tableNum;
		} catch (SQLException e) {
			throw new DBClusterException("读取shardcluster表失败", e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				LOG.error(e);
			}
		}
	}

	/**
	 * 获取零库.
	 * 
	 * @return 零库数据源.
	 */
	private DataSource _getZero(String clusterName) {
		List<DataSource> dsList = masterDSCluster.get(clusterName);
		if (dsList == null || dsList.isEmpty()) {
			throw new RuntimeException("找不到零库, dbname=" + clusterName);
		}

		DataSource ds = dsList.get(0);
		return ds;
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
			config = XmlClusterConfigImpl.getInstance();
		}

		return config;
	}

	/**
	 * 创建数据源连接.
	 */
	public abstract DataSource buildDataSource(DBConnectionInfo dbConnInfo);

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
	public Map<String, List<DataSource>> getMasterDsCluster() {
		return this.masterDSCluster;
	}

	@Override
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster() {
		return this.tableCluster;
	}

}
