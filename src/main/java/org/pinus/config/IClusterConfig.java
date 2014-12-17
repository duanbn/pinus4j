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

package org.pinus.config;

import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.pinus.api.enums.EnumDbConnectionPoolCatalog;
import org.pinus.cluster.beans.DBClusterInfo;
import org.pinus.cluster.enums.HashAlgoEnum;

/**
 * 存储中间件配置信息接口. 此接口提供的信息都是通过配置来获取.
 * 一个storage-config.properties中可以配置多个数据库集群，每个数据库集群又可以配置一个主库集群和多个从库集群.
 * 
 * @author duanbn
 */
public interface IClusterConfig {

	/**
	 * 获取数据库连接方式.
	 * 
	 * @return
	 */
	public EnumDbConnectionPoolCatalog getDbConnectionPoolCatalog();

	/**
	 * 获取ID生成器默认批量生成值.
	 * 
	 * @return
	 */
	public int getIdGeneratorBatch();

	/**
	 * 获取zookeeper客户端
	 * 
	 * @return
	 */
	public ZooKeeper getZooKeeper();

	/**
	 * 获取配置的hash算法.
	 * 
	 * @return hash算法枚举
	 */
	public HashAlgoEnum getHashAlgo();

	/**
	 * 获取DB集群信息
	 * 
	 * @return
	 */
	public Map<String, DBClusterInfo> getDBClusterInfo();

	/**
	 * 获取xml中配置的zookeeper连接
	 * 
	 * @return
	 */
	public String getZookeeperUrl();

}
