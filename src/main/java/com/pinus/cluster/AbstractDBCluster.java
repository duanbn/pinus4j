package com.pinus.cluster;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.pinus.api.IShardingValue;
import com.pinus.api.enums.EnumDB;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.route.DBRouteInfo;
import com.pinus.cluster.route.IClusterRouter;
import com.pinus.config.IClusterConfig;
import com.pinus.config.impl.XmlClusterConfigImpl;
import com.pinus.constant.Const;
import com.pinus.exception.DBClusterException;
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

	// 加载主库集群
	Map<String, List<DBClusterInfo>> masterDbCluster;

	// 加载从库集群
	Map<String, List<List<DBClusterInfo>>> slaveDbCluster;

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

		// 加载主库集群
		masterDbCluster = config.loadMasterDbClusterInfo();
		// 加载从库集群
		slaveDbCluster = config.loadSlaveDbClusterInfo();

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
			// 初始化主集群连接
			_initMasterCluster(this.masterDbCluster);

			// 初始化从集群连接
			_initSlaveCluster(this.slaveDbCluster);

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
			// 关闭主库集群连接
			for (Map.Entry<String, List<DBClusterInfo>> entry : this.masterDbCluster.entrySet()) {
				for (DBClusterInfo dbClusterInfo : entry.getValue()) {
					closeDataSource(dbClusterInfo.getGlobalConnInfo());

					for (DBConnectionInfo connInfo : dbClusterInfo.getDbConnInfos()) {
						closeDataSource(connInfo);
					}
				}
			}

			// 关闭从库集群连接
			for (Map.Entry<String, List<List<DBClusterInfo>>> entry : this.slaveDbCluster.entrySet()) {
				for (List<DBClusterInfo> clusterInfos : entry.getValue()) {
					for (DBClusterInfo clusterInfo : clusterInfos) {
						closeDataSource(clusterInfo.getGlobalConnInfo());

						for (DBConnectionInfo connInfo : clusterInfo.getDbConnInfos()) {
							closeDataSource(connInfo);
						}
					}
				}
			}
		} catch (Exception e) {
			throw new DBClusterException("关闭数据库集群失败", e);
		}
	}

	@Override
	public List<DBConnectionInfo> getMasterGlobalDbConn(String clusterName) {
		List<DBClusterInfo> dbClusterInfos = this.masterDbCluster.get(clusterName);

		List<DBConnectionInfo> result = new ArrayList<DBConnectionInfo>();

		for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
			result.add(dbClusterInfo.getGlobalConnInfo());
		}

		return result;
	}

	@Override
	public DBConnectionInfo getMasterGlobalDbConn(Number pk, String clusterName) {
		List<DBClusterInfo> dbClusterInfos = this.masterDbCluster.get(clusterName);

		long pkValue = pk.longValue();
		DBClusterInfo rangeClusterInfo = null;
		for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
			if (dbClusterInfo.getStart() <= pkValue && dbClusterInfo.getEnd() >= pkValue) {
				rangeClusterInfo = dbClusterInfo;
				break;
			}
		}
		if (rangeClusterInfo == null) {
			throw new IllegalArgumentException("找不到主全局库, clusterName=" + clusterName + ", pk=" + pkValue);
		}

		return rangeClusterInfo.getGlobalConnInfo();
	}

	@Override
	public Map<DBConnectionInfo, List<Number>> getMasterGlobalDbConn(Number[] pks, String clusterName) {
		List<DBClusterInfo> dbClusterInfos = this.masterDbCluster.get(clusterName);

		Map<DBConnectionInfo, List<Number>> result = new HashMap<DBConnectionInfo, List<Number>>();
		for (Number pkNumber : pks) {
			long pk = pkNumber.longValue();

			DBClusterInfo rangeClusterInfo = null;
			for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
				if (dbClusterInfo.getStart() <= pk && dbClusterInfo.getEnd() >= pk) {
					rangeClusterInfo = dbClusterInfo;
					break;
				}
			}
			if (rangeClusterInfo == null) {
				throw new IllegalArgumentException("找不到主全局库, clusterName=" + clusterName + ", pk=" + pk);
			}

			List list = null;
			if (result.get(rangeClusterInfo) != null) {
				list = result.get(rangeClusterInfo);
			} else {
				list = new ArrayList();
			}
			list.add(pk);
			result.put(rangeClusterInfo.getGlobalConnInfo(), list);
		}

		return result;
	}

	@Override
	public Map<DBConnectionInfo, List> getMasterGlobalDbConn(List entities, String clusterName) {
		List<DBClusterInfo> dbClusterInfos = this.masterDbCluster.get(clusterName);

		Map<DBConnectionInfo, List> result = new HashMap<DBConnectionInfo, List>();
		for (Object entity : entities) {
			long pk = ReflectUtil.getPkValue(entity).longValue();

			DBClusterInfo rangeClusterInfo = null;
			for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
				if (dbClusterInfo.getStart() <= pk && dbClusterInfo.getEnd() >= pk) {
					rangeClusterInfo = dbClusterInfo;
					break;
				}
			}
			if (rangeClusterInfo == null) {
				throw new IllegalArgumentException("找不到主全局库, clusterName=" + clusterName + ", pk=" + pk);
			}

			List list = null;
			if (result.get(rangeClusterInfo) != null) {
				list = result.get(rangeClusterInfo);
			} else {
				list = new ArrayList();
			}
			list.add(entity);
			result.put(rangeClusterInfo.getGlobalConnInfo(), list);
		}

		return result;
	}

	@Override
	public DBConnectionInfo getSlaveGlobalDbConn(Number pk, String clusterName, EnumDBMasterSlave slave) {
		List<DBClusterInfo> dbClusterInfos = this.slaveDbCluster.get(clusterName).get(slave.getValue());

		long pkValue = pk.longValue();

		DBClusterInfo rangeClusterInfo = null;
		for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
			if (dbClusterInfo.getStart() <= pkValue && dbClusterInfo.getEnd() >= pkValue) {
				rangeClusterInfo = dbClusterInfo;
				break;
			}
		}

		if (rangeClusterInfo == null) {
			throw new IllegalArgumentException("找不到从全局库, clusterName=" + clusterName + ", slave num="
					+ slave.getValue());
		}

		return rangeClusterInfo.getGlobalConnInfo();
	}

	@Override
	public List<DBConnectionInfo> getSlaveGlobalDbConn(String clusterName, EnumDBMasterSlave slave) {
		List<DBClusterInfo> dbClusterInfos = this.slaveDbCluster.get(clusterName).get(slave.getValue());

		List<DBConnectionInfo> result = new ArrayList<DBConnectionInfo>();

		for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
			result.add(dbClusterInfo.getGlobalConnInfo());
		}

		return result;
	}

	@Override
	public Map<DBConnectionInfo, List> getSlaveGlobalDbConn(List entities, String clusterName, EnumDBMasterSlave slave) {
		List<List<DBClusterInfo>> oneSlaves = this.slaveDbCluster.get(clusterName);

		Map<DBConnectionInfo, List> result = new HashMap<DBConnectionInfo, List>();

		List<DBClusterInfo> oneSlave = oneSlaves.get(slave.getValue());

		for (Object entity : entities) {
			long pk = ReflectUtil.getPkValue(entity).longValue();

			DBClusterInfo rangeClusterInfo = null;
			for (DBClusterInfo dbClusterInfo : oneSlave) {
				if (dbClusterInfo.getStart() <= pk && dbClusterInfo.getEnd() >= pk) {
					rangeClusterInfo = dbClusterInfo;
					break;
				}
			}

			if (rangeClusterInfo == null) {
				throw new IllegalArgumentException("找不到从全局库, clusterName=" + clusterName + ", slave num="
						+ slave.getValue());
			}

			List list = null;
			if (result.get(rangeClusterInfo) != null) {
				list = result.get(rangeClusterInfo);
			} else {
				list = new ArrayList();
			}
			list.add(entity);
			result.put(rangeClusterInfo.getGlobalConnInfo(), list);
		}

		return result;
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

		List<DBClusterInfo> dbClusterInfos = this.masterDbCluster.get(clusterName);
		DBClusterInfo dbClusterInfo = dbClusterInfos.get(routeInfo.getClusterIndex());
		if (dbClusterInfo == null) {
			throw new DBClusterException("找不到数据库集群, db name=" + clusterName);
		}
		List<DBConnectionInfo> dbList = dbClusterInfo.getDbConnInfos();
		if (dbList == null || dbList.isEmpty()) {
			throw new DBClusterException("找不到数据库集群, db name=" + clusterName);
		}

		Connection dbConn = null;
		try {
			dbConn = dbList.get(dbIndex).getDatasource().getConnection();
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
		db.setStart(dbClusterInfo.getStart());
		db.setEnd(dbClusterInfo.getEnd());

		return db;
	}

	@Override
	public DB selectDbFromSlave(EnumDBMasterSlave slaveNum, String tableName, IShardingValue<?> value)
			throws DBClusterException {

		// 选择分库
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

		List<List<DBClusterInfo>> slaveClusters = this.slaveDbCluster.get(clusterName);
		List<DBClusterInfo> slaveCluster = slaveClusters.get(slaveNum.getValue());
		DBClusterInfo dbClusterInfo = slaveCluster.get(routeInfo.getClusterIndex());
		if (dbClusterInfo == null) {
			throw new DBClusterException("找不到数据库集群, db name=" + clusterName);
		}

		// 获取数据库连接
		Connection dbConn = null;
		try {
			dbConn = dbClusterInfo.getDbConnInfos().get(dbIndex).getDatasource().getConnection();
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
		db.setStart(dbClusterInfo.getStart());
		db.setEnd(dbClusterInfo.getEnd());

		return db;
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
					for (DBClusterInfo dbClusterInfo : this.masterDbCluster.get(clusterName)) {
						Connection dbConn = dbClusterInfo.getDbConnInfos().get(dbIndex).getDatasource().getConnection();
						LOG.debug("开始创建主库库表, 库名:" + dbConn.getCatalog());
						long start = System.currentTimeMillis();
						int tableNum = oneDbTables.get(dbIndex).get(table.getName());
						this.dbGenerator.syncTable(dbConn, table, tableNum);
						dbConn.close();
						LOG.debug("创建完毕， 耗时" + (System.currentTimeMillis() - start) + "ms");
					}
				}

				// 创建从库库表
				List<List<DBClusterInfo>> slaveDbs = this.slaveDbCluster.get(clusterName);
				if (slaveDbs == null) {
					continue;
				}
				for (List<DBClusterInfo> slaveDbClusterInfos : slaveDbs) {
					for (int i = 0; i < slaveDbClusterInfos.size(); i++) {
						for (Integer dbIndex : oneDbTables.keySet()) {
							Connection dbConn = slaveDbClusterInfos.get(i).getDbConnInfos().get(dbIndex)
									.getDatasource().getConnection();
							LOG.debug("开始创建从库库表, 库名:" + dbConn.getCatalog() + ", 从库号:" + i);
							long start = System.currentTimeMillis();
							int tableNum = oneDbTables.get(dbIndex).get(table.getName());
							this.dbGenerator.syncTable(dbConn, table, tableNum);
							dbConn.close();
							LOG.debug("创建完毕， 耗时" + (System.currentTimeMillis() - start) + "ms");
						}
					}
				}
			} else { // 当ShardingNumber等于0时表示全局表
				// 全局主库
				List<DBClusterInfo> masterClusterInfos = this.masterDbCluster.get(clusterName);
				for (DBClusterInfo dbClusterInfo : masterClusterInfos) {
					DataSource globalDs = dbClusterInfo.getGlobalConnInfo().getDatasource();
					if (globalDs != null) {
						Connection conn = globalDs.getConnection();
						this.dbGenerator.syncTable(conn, table);
						conn.close();
					}
				}
				// 全局从库
				List<List<DBClusterInfo>> slaveDs = this.slaveDbCluster.get(clusterName);
				if (slaveDs != null) {
					for (List<DBClusterInfo> dbClusterInfos : slaveDs) {
						for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
							Connection conn = dbClusterInfo.getGlobalConnInfo().getDatasource().getConnection();
							this.dbGenerator.syncTable(conn, table);
							conn.close();
						}
					}
				}
			}

		}
	}

	private void _initMasterCluster(Map<String, List<DBClusterInfo>> masterCluster) {
		for (List<DBClusterInfo> dbClusterInfos : masterCluster.values()) {
			for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
				buildDataSource(dbClusterInfo.getGlobalConnInfo());

				for (DBConnectionInfo dbConnInfo : dbClusterInfo.getDbConnInfos()) {
					buildDataSource(dbConnInfo);
				}
			}
		}
	}

	private void _initSlaveCluster(Map<String, List<List<DBClusterInfo>>> slaveCluster) {
		for (List<List<DBClusterInfo>> dbClusterInfos : slaveCluster.values()) {
			for (List<DBClusterInfo> dbClusterInfo : dbClusterInfos) {
				for (DBClusterInfo value : dbClusterInfo) {
					buildDataSource(value.getGlobalConnInfo());

					for (DBConnectionInfo dbConnInfo : value.getDbConnInfos()) {
						buildDataSource(dbConnInfo);
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
	private void _initTableCluster(Map<String, List<DBClusterInfo>> masterDbCluster, List<DBTable> tables)
			throws DBClusterException {

		// {分库下标, {表名, 分表数}}
		Map<Integer, Map<String, Integer>> oneDbTable = null;

		Map<String, Integer> tableNum = null;
		for (Map.Entry<String, List<DBClusterInfo>> clusterInfo : masterDbCluster.entrySet()) {
			oneDbTable = new HashMap<Integer, Map<String, Integer>>();

			String clusterName = clusterInfo.getKey();

			int dbNum = clusterInfo.getValue().get(0).getDbConnInfos().size();

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
			config = XmlClusterConfigImpl.getInstance();
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
	public Map<String, List<DBClusterInfo>> getMasterCluster() {
		return this.masterDbCluster;
	}

	@Override
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster() {
		return this.tableCluster;
	}

}
