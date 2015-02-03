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

package org.pinus4j.cluster.router.impl;

import java.util.List;

import org.pinus4j.api.IShardingKey;
import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.ITableCluster;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBClusterRegionInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.enums.HashAlgoEnum;
import org.pinus4j.cluster.router.IClusterRouter;
import org.pinus4j.cluster.router.RouteInfo;
import org.pinus4j.exceptions.DBRouteException;

/**
 * 抽象的数据库集群路由实现. 持有数据库的集群信息，子类专注于实现路由算法.
 * 
 * @author duanbn
 */
public abstract class AbstractClusterRouter implements IClusterRouter {

	/**
	 * hash 算法
	 */
	private HashAlgoEnum hashAlgo;

	/**
	 * db cluster.
	 */
	private IDBCluster dbCluster;

	/**
	 * 数据表集群.
	 */
	private ITableCluster tableCluster;

	@Override
	public void setHashAlgo(HashAlgoEnum algoEnum) {
		this.hashAlgo = algoEnum;
	}

	@Override
	public HashAlgoEnum getHashAlgo() {
		return this.hashAlgo;
	}

	@Override
	public void setDBCluster(IDBCluster dbCluster) {
		this.dbCluster = dbCluster;
	}

	@Override
	public IDBCluster getDBCluster() {
		return this.dbCluster;
	}

	@Override
	public void setTableCluster(ITableCluster tableCluster) {
		this.tableCluster = tableCluster;
	}

	@Override
	public ITableCluster getTableCluster() {
		return this.tableCluster;
	}

	@Override
	public RouteInfo select(EnumDBMasterSlave clusterType, String tableName, IShardingKey<?> value)
			throws DBRouteException {
		RouteInfo dbRouteInfo = new RouteInfo();

		long shardingValue = getShardingValue(value);
		String clusterName = value.getClusterName();

		// find cluster info.
		DBClusterInfo dbClusterInfo = this.dbCluster.getDBClusterInfo(clusterName);
		if (dbClusterInfo == null) {
			throw new IllegalStateException("can not found cluster " + clusterName);
		}

		// compute and find cluster region info.
		List<DBClusterRegionInfo> regionInfos = dbClusterInfo.getDbRegions();
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
			throw new DBRouteException("find db cluster failure, over capacity, cluster name is " + clusterName
					+ ", sharding value is " + shardingValue);
		}

		// compute and find database instance.
		List<DBInfo> dbInfos = null;
		switch (clusterType) {
		case MASTER:
			dbInfos = regionInfo.getMasterDBInfos();
			break;
		default:
			// get multi slave info.
			List<List<DBInfo>> multiSlaveDBInfos = regionInfo.getSlaveDBInfos();
			if (multiSlaveDBInfos == null || multiSlaveDBInfos.isEmpty()) {
				throw new DBRouteException("find slave db cluster failure cluster name is " + clusterName);
			}
			int slaveIndex = clusterType.getValue();
			dbInfos = multiSlaveDBInfos.get(slaveIndex);
			break;
		}

		// do select
		if (dbInfos == null || dbInfos.isEmpty()) {
			throw new DBRouteException("find db cluster failure, cluster name is " + clusterName);
		}

		DBInfo dbInfo = doSelect(dbInfos, value);

		dbRouteInfo.setDbInfo(dbInfo);
		dbRouteInfo.setClusterName(clusterName);
		dbRouteInfo.setRegionIndex(regionIndex);

		// compute and find table
		try {
			// get table number.
			int tableNum = tableCluster.getTableNumber(clusterName, tableName);

			// compute table index.
			int tableIndex = (int) shardingValue % tableNum;

			dbRouteInfo.setTableName(tableName);
			dbRouteInfo.setTableIndex(tableIndex);
		} catch (Exception e) {
			throw new DBRouteException("find table failure, cluster name is " + dbRouteInfo.getClusterName()
					+ "db name is " + dbRouteInfo.getDbInfo().getDbName() + ", table name is " + tableName);
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
	 * select database instance.
	 *
	 * @param dbInfos
	 *            database cluster info.
	 * @param value
	 *            sharding value.
	 *
	 * @return index of database info list.
	 */
	protected abstract DBInfo doSelect(List<DBInfo> dbInfos, IShardingKey<?> value) throws DBRouteException;
}
