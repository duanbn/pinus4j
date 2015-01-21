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

package org.pinus.cluster.route;

import java.util.Map;

import org.pinus.api.IShardingKey;
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.cluster.ITableCluster;
import org.pinus.cluster.beans.DBClusterInfo;
import org.pinus.cluster.enums.HashAlgoEnum;
import org.pinus.exception.DBRouteException;

/**
 * 数据库集群路由器. 负责依据ShardingValue的值来找到合适库表. 不同的路由算法都必须实现此接口. 三种配置信息获取方式，1.
 * 从classpath根路径的storage-config.properties中获取。 2. 从指定的文件中获取。 3. 从zookeeper中获取.
 * 优先从zookeeper中加载，其次从指定的文件，默认从classpath根路径
 * 
 * @author duanbn
 */
public interface IClusterRouter {

	/**
	 * 设置hash算法
	 * 
	 * @param algoEnum
	 */
	public void setHashAlgo(HashAlgoEnum algoEnum);

	/**
	 * 获取hash算法
	 * 
	 * @return
	 */
	public HashAlgoEnum getHashAlgo();

	/**
	 * 设置主库集群.
	 * 
	 * @param dbClusterInfo
	 *            集群信息.
	 */
	public void setDbClusterInfo(Map<String, DBClusterInfo> dbClusterInfo);

	/**
	 * 设置数据表集群.
	 * 
	 * @param tableCluster
	 */
	public void setTableCluster(ITableCluster tableCluster);

	/**
	 * 获取数据表集群.
	 * 
	 * @return 数据表集群信息
	 */
	public ITableCluster getTableCluster();

	/**
	 * 选择需要操作的数据库表.
	 * 
	 * @param clusterType
	 *            主从库类型.
	 * @param tableName
	 *            表名.
	 * @param value
	 *            分库分表因子.
	 * 
	 * @return 命中的分库分表信息.
	 * 
	 * @throws DBRouteException
	 *             路由操作失败
	 */
	public DBRouteInfo select(EnumDBMasterSlave clusterType, String tableName, IShardingKey<?> value)
			throws DBRouteException;

}
