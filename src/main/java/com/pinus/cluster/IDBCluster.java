package com.pinus.cluster;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.pinus.api.IShardingValue;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.route.IClusterRouter;
import com.pinus.exception.DBClusterException;
import com.pinus.generator.IDBGenerator;

/**
 * 数据库集群. 数据库集群主要类，持有所有的数据库集群信息，保存集群的数据库连接包括主库和从库。
 * 
 * @author duanbn
 */
public interface IDBCluster {

	/**
	 * 启动集群. 调用数据库集群前需要调用此方法，为了初始化集群连接.
	 * 
	 * @throws DBClusterException
	 *             初始化失败
	 */
	public void startup() throws DBClusterException;

	/**
	 * 关闭集群. 系统停止时关闭数据库集群.
	 * 
	 * @throws DBClusterException
	 *             关闭失败
	 */
	public void shutdown() throws DBClusterException;

	/**
	 * 获取主全局库连接.
	 * 
	 * @return 数据库连接.
	 */
	public Connection getMasterGlobalDbConn(String clusterName) throws SQLException;

	/**
	 * 获取从全局库连接.
	 * 
	 * @return 数据库连接.
	 */
	public Connection getSlaveGlobalDbConn(String clusterName, EnumDBMasterSlave slave) throws SQLException;

	/**
	 * 设置数据库路由器.
	 */
	public void setDbRouter(IClusterRouter dbRouter);

	/**
	 * 获取数据库路由器.
	 */
	public IClusterRouter getDbRouter();

	/**
	 * 获取主库的零号库信息.
	 * 
	 * @param clusterName
	 *            数据库集群名称
	 * 
	 * @return 主库的零号库
	 * 
	 * @throws DBClusterException
	 *             获取失败
	 */
	public DB getGlobalIdFromMaster(String clusterName) throws DBClusterException;

	/**
	 * 从主库集群中获取被操作的库表.
	 * 
	 * @param tableName
	 *            数据表名
	 * @param value
	 *            分库分表因子.
	 * @return 被操作的库表
	 */
	public DB selectDbFromMaster(String tableName, IShardingValue<?> value) throws DBClusterException;

	/**
	 * 从从库集群中获取被操作的库表.
	 * 
	 * @param slave
	 *            从库
	 * @param tableName
	 *            数据库表名
	 * @param value
	 *            分库分表因子
	 * @return 被操作的库表
	 */
	public DB selectDbFromSlave(EnumDBMasterSlave slave, String tableName, IShardingValue<?> value)
			throws DBClusterException;

	/**
	 * 设置是否创建表.
	 * 
	 * @param isCreateTable
	 *            true:创建, false:不创建
	 */
	public void setCreateTable(boolean isCreateTable);

	/**
	 * 是否创建表.
	 * 
	 * @return true:创建, false:不创建
	 */
	public boolean isCreateTable();

	/**
	 * 设置数据库表生成器.
	 * 
	 * @param dbGenerator
	 *            数据库表生成器
	 */
	public void setDbGenerator(IDBGenerator dbGenerator);

	/**
	 * 设置需要扫描的实体对象包.
	 * 
	 * @param scanPackage
	 *            包名
	 */
	public void setScanPackage(String scanPackage);

	/**
	 * 获取集群数据源
	 * 
	 * @return 集群数据源
	 */
	public Map<String, List<DataSource>> getMasterDsCluster();

	/**
	 * 获取集群表集合
	 * 
	 * @return 集群表集合
	 */
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster();

}
