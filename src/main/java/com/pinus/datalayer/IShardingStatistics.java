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

package com.pinus.datalayer;

import com.pinus.cluster.IDBCluster;
import com.pinus.datalayer.beans.DBClusterStatInfo;

/**
 * 集群统计相关的接口.
 * 
 * @author duanbn
 * 
 */
public interface IShardingStatistics {

	/**
	 * 统计单个实体的信息
	 * 
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            实体对象
	 * @return 统计信息
	 */
	public DBClusterStatInfo statEntity(String clusterName, Class<?> clazz);
	
	public IDBCluster getDbCluster();
	
	public void setDbCluster(IDBCluster dbCluster);
	
}
