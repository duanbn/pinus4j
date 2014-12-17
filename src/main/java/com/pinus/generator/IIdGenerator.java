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

package com.pinus.generator;

import com.pinus.exception.DBOperationException;

/**
 * 集群全局唯一id生成器接口. 在集群中对单个数据对象生成整个集群的全局唯一id.
 * 
 * @author duanbn
 */
public interface IIdGenerator {

    /**
     * 需要关闭zookeeper的连接.
     */
    public void close();

	/**
	 * 生成全局唯一的int id. 对一个数据对象的集群全局唯一id.
	 * 
	 * @param dbCluster
	 *            生成全局唯一id的数据库集群
	 * @param clusterName
	 *            数据库集群名
	 * @param name
	 *            id生成的名字
	 * 
	 * @return 单个数据对象的集群全局唯一id
	 * 
	 * @throws DBOperationException
	 *             生成id失败
	 */
	public int genClusterUniqueIntId(String clusterName, String name);

	/**
	 * 生成全局唯一的long id. 对一个数据对象的集群全局唯一id.
	 * 
	 * @param dbCluster
	 *            生成全局唯一id的数据库集群
	 * @param clusterName
	 *            数据库集群名
	 * @param name
	 *            id生成的名字
	 * 
	 * @return 单个数据对象的集群全局唯一id
	 * 
	 * @throws DBOperationException
	 *             生成id失败
	 */
	public long genClusterUniqueLongId(String clusterName, String name);

	/**
	 * 批量生成全局唯一主键.
	 * 
	 * @param dbCluster
	 *            生成全局唯一id的数据库集群
	 * @param clusterName
	 *            数据库集群名
	 * @param name
	 *            id生成的名字
	 * @param batchSize
	 *            批量数
	 */
	public long[] genClusterUniqueLongIdBatch(String clusterName, String name, int batchSize);

	/**
	 * 批量生成全局唯一主键.
	 * 
	 * @param dbCluster
	 *            生成全局唯一id的数据库集群
	 * @param clusterName
	 *            数据库集群名
	 * @param name
	 *            id生成的名字
	 * @param batchSize
	 *            批量数
	 */
	public int[] genClusterUniqueIntIdBatch(String clusterName, String name, int batchSize);

}
