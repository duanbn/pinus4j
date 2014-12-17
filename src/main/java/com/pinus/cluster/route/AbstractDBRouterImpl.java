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

package com.pinus.cluster.route;

import java.util.List;
import java.util.Map;

import com.pinus.api.IShardingKey;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBClusterRegionInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.enums.HashAlgoEnum;
import com.pinus.exception.DBRouteException;

/**
 * 抽象的数据库集群路由实现. 持有数据库的集群信息，子类专注于实现路由算法.
 * 
 * @author duanbn
 */
public abstract class AbstractDBRouterImpl implements IClusterRouter {

	/**
	 * hash 算法
	 */
	private HashAlgoEnum hashAlgo;

	/**
	 * 主库集群.
	 */
	private Map<String, DBClusterInfo> dbClusterInfo;

	/**
	 * 数据表集群. {库名, {库下标, {表名, 表个数}}}
	 */
	private Map<String, Map<Integer, Map<String, Integer>>> tableCluster;

	@Override
	public void setHashAlgo(HashAlgoEnum algoEnum) {
		this.hashAlgo = algoEnum;
	}

	@Override
	public HashAlgoEnum getHashAlgo() {
		return this.hashAlgo;
	}

	@Override
	public void setDbClusterInfo(Map<String, DBClusterInfo> dbClusterInfo) {
		this.dbClusterInfo = dbClusterInfo;
	}

	@Override
	public void setTableCluster(Map<String, Map<Integer, Map<String, Integer>>> tableCluster) {
		this.tableCluster = tableCluster;
	}

	@Override
	public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster() {
		return this.tableCluster;
	}

	@Override
	public DBRouteInfo select(EnumDBMasterSlave clusterType, String tableName, IShardingKey<?> value)
			throws DBRouteException {
		DBRouteInfo dbRouteInfo = null;

		long shardingValue = getShardingValue(value);
		String clusterName = value.getClusterName();
		List<DBClusterRegionInfo> regionInfos = this.dbClusterInfo.get(clusterName).getDbRegions();

		if (regionInfos == null || regionInfos.isEmpty()) {
			throw new DBRouteException("查找集群失败, clustername=" + clusterName);
		}

		DBClusterRegionInfo regionInfo = null;
		int regionIndex = 0;
		for (DBClusterRegionInfo region : regionInfos) {
			if (region.getStart() <= shardingValue && region.getEnd() >= shardingValue) {
				regionInfo = region;
				break;
			}
			regionIndex++;
		}
		if (regionInfo == null) {
			throw new DBRouteException("查找集群失败, 超出容量, dbname=" + clusterName + ", shardingvalue=" + shardingValue);
		}

		switch (clusterType) {
		case MASTER:
			if (regionInfo.getMasterConnection() == null || regionInfo.getMasterConnection().isEmpty()) {
				throw new DBRouteException("查找集群失败, clustername=" + clusterName);
			}
			dbRouteInfo = doSelectFromMaster(regionInfo.getMasterConnection(), value);
			break;
		default:
			int slaveIndex = clusterType.getValue();
			dbRouteInfo = doSelectFromSlave(regionInfo.getSlaveConnection(), slaveIndex, value);
			break;
		}
		if (dbRouteInfo == null) {
			throw new RuntimeException("路由操作失败， 找不到相关的库表, clusterType=" + clusterType + ", sharding value=" + value);
		}

		dbRouteInfo.setClusterName(clusterName);
		dbRouteInfo.setRegionIndex(regionIndex);

		// 计算分表.
		try {
			Map<Integer, Map<String, Integer>> dbCluster = tableCluster.get(dbRouteInfo.getClusterName());
			Map<String, Integer> tableCluster = dbCluster.get(dbRouteInfo.getDbIndex());
			int tableNum = tableCluster.get(tableName);

			// 计算分表下标
			int tableIndex = (int) shardingValue % tableNum;

			dbRouteInfo.setTableName(tableName);
			dbRouteInfo.setTableIndex(tableIndex);
		} catch (Exception e) {
			throw new DBRouteException("路由操作失败, 找不到可以用的库表, dbname=" + dbRouteInfo.getClusterName() + ", dbindex="
					+ dbRouteInfo.getDbIndex() + ", tablename=" + tableName);
		}

		return dbRouteInfo;
	}

	/**
	 * 获取shardingvalue的值，如果是String则转成long
	 * 
	 * @param shardingValue
	 * @param mod
	 * @return
	 */
	protected long getShardingValue(IShardingKey<?> value) {
		Object shardingValue = value.getValue();

		if (shardingValue instanceof String) {
			return (int) this.hashAlgo.hash((String) shardingValue);
		} else if (shardingValue instanceof Integer) {
			return (Integer) shardingValue;
		} else if (shardingValue instanceof Long) {
			return (Long) shardingValue;
		} else {
			throw new IllegalArgumentException("sharding value的值只能是String或者Number " + shardingValue);
		}
	}

	/**
	 * 路由操作. 从主库集群中获取目标库表.
	 * 
	 * @param dbMasterCluster
	 *            主库集群信息.
	 * @param value
	 *            分库分表因子
	 * @return 路由结果
	 */
	protected abstract DBRouteInfo doSelectFromMaster(List<DBConnectionInfo> masterConnections, IShardingKey<?> value)
			throws DBRouteException;

	/**
	 * 路由操作. 从从库中获取路由库表.
	 * 
	 * @param dbSlaveCluster
	 *            从库集群信息
	 * @param value
	 *            分库分表因子
	 * @return 路由结果
	 */
	protected abstract DBRouteInfo doSelectFromSlave(List<List<DBConnectionInfo>> slaveConnection, int slaveIndex,
			IShardingKey<?> value) throws DBRouteException;

}
