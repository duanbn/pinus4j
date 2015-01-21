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

package org.pinus.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.pinus.api.IShardingKey;
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.api.enums.EnumDBRouteAlg;
import org.pinus.api.enums.EnumSyncAction;
import org.pinus.cluster.beans.DBClusterInfo;
import org.pinus.cluster.beans.DBInfo;
import org.pinus.cluster.route.IClusterRouter;
import org.pinus.config.IClusterConfig;
import org.pinus.datalayer.IDataLayerBuilder;
import org.pinus.exception.DBClusterException;
import org.pinus.generator.IDBGenerator;
import org.pinus.generator.IIdGenerator;
import org.pinus.generator.beans.DBTable;

/**
 * 数据库集群. 数据库集群主要类，持有所有的数据库集群信息，保存集群的数据库连接包括主库和从库。 初始化集群的方法，<br/>
 * 当没有设置scanpakcage时，从zookeeper中加载. 已DbcpDBClusterImpl实现为例<br/>
 * 
 * <pre>
 * IDBCluster dbCluster = new DbcpDBClusterImpl(); </br>
 * dbCluster.setDbRouteAlg(EnumDBRouteAlg); // 设置分片路由算法. 可选
 * dbCluster.setScanPackage("entity full path package"); // 可选
 * dbCluster.setShardInfoFromZk(true | false); // 默认为false, 可选
 * dbCluster.startup();
 * </pre>
 * 
 * @author duanbn
 */
public interface IDBCluster {
	
	public Lock createLock(String name);

	/**
	 * get data layer component builder.
	 * 
	 * @see IDataLayerBuilder
	 */
	public IDataLayerBuilder getDataLayerBuilder();

	/**
	 * 设置此集群是否从zookeeper中加载分片信息.
	 *
	 * @param value
	 *            true:是， false:否.
	 */
	public void setShardInfoFromZk(boolean value);

	/**
	 * 从Zookeeper中获取分片信息.
	 *
	 * @return 分片信息.
	 */
	public List<DBTable> getDBTableFromZk();

	/**
	 * 从Jvm中获取分片信息.
	 *
	 * @return 分片信息.
	 */
	public List<DBTable> getDBTableFromJvm();

	/**
	 * Get all info about this cluster.
	 *
	 * @return all cluster info.
	 */
	public Collection<DBClusterInfo> getDbClusterInfo();

	/**
	 * 获取集群信息.
	 * 
	 * @param clusterName
	 *            集群名
	 * @return 集群信息
	 */
	public DBClusterInfo getDbClusterInfo(String clusterName);

	/**
	 * 启动集群. 调用数据库集群前需要调用此方法，为了初始化集群连接.
	 * 
	 * @throws DBClusterException
	 *             初始化失败
	 */
	public void startup() throws DBClusterException;

	/**
	 * 启动集群. 调用数据库集群前需要调用此方法，为了初始化集群连接.
	 * 
	 * @param xmlFilePath
	 *            配置文件绝对路径
	 * 
	 * @throws DBClusterException
	 *             初始化失败
	 */
	public void startup(String xmlFilePath) throws DBClusterException;

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
	 * @param clusterName
	 * @return
	 */
	public DBInfo getMasterGlobalConn(String clusterName) throws DBClusterException;

	/**
	 * 获取从库的全局库连接
	 * 
	 * @param clusterName
	 * @param slave
	 * @return
	 */
	public DBInfo getSlaveGlobalDbConn(String clusterName, EnumDBMasterSlave slave) throws DBClusterException;

	/**
	 * 从主库集群中获取被操作的库表.
	 * 
	 * @param tableName
	 *            数据表名
	 * @param value
	 *            分库分表因子.
	 * @return 被操作的库表
	 */
	public DB selectDbFromMaster(String tableName, IShardingKey<?> value) throws DBClusterException;

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
	public DB selectDbFromSlave(String tableName, IShardingKey<?> value, EnumDBMasterSlave slave)
			throws DBClusterException;

	/**
	 * 获取所有的分片引用.
	 *
	 * @param tableNum
	 *            分表数
	 * @param clusterName
	 *            分片名称
	 * @param tableName
	 *            表名
	 *
	 * @return 所有分片
	 */
	public List<DB> getAllMasterShardingDB(int tableNum, String clusterName, String tableName);

	/**
	 * 获取此实体对象对应的所有的分库分表引用.
	 * 
	 * @param clazz
	 *            数据对象
	 * @return
	 */
	public List<DB> getAllMasterShardingDB(Class<?> clazz);

	/**
	 * 获取集群从库列表.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param slave
	 *            从库号
	 */
	public List<DB> getAllSlaveShardingDB(Class<?> clazz, EnumDBMasterSlave slave);

	/**
	 * 获取数据库路由器.
	 */
	public IClusterRouter getDbRouter();

	/**
	 * 设置路由算法.
	 */
	public void setDbRouteAlg(EnumDBRouteAlg routeAlg);

	/**
	 * 获取路由算法.
	 */
	public EnumDBRouteAlg getDbRouteAlg();

	/**
	 * 设置数据表同步动作.
	 * 
	 * @param syncAction
	 */
	public void setSyncAction(EnumSyncAction syncAction);

	/**
	 * 获取db生成器.
	 */
	public IDBGenerator getDbGenerator();

	/**
	 * 获取id生成器.
	 * 
	 * @return
	 */
	public IIdGenerator getIdGenerator();

	/**
	 * 设置需要扫描的实体对象包.
	 * 
	 * @param scanPackage
	 *            包名
	 */
	public void setScanPackage(String scanPackage);

	/**
	 * 获取集群表集合. 集群中的表集合. {集群名称, {分库下标, {表名, 分表数}}}
	 * 
	 * @return 集群表集合
	 */
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster();

	/**
	 * 获取集群配置.
	 * 
	 * @return
	 */
	public IClusterConfig getClusterConfig();

}
