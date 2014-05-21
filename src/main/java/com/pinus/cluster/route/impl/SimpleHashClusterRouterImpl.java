package com.pinus.cluster.route.impl;

import java.util.List;
import java.util.Map;

import com.pinus.api.IShardingValue;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.route.AbstractDBRouterImpl;
import com.pinus.cluster.route.DBRouteInfo;
import com.pinus.exception.DBRouteException;

/**
 * 基于取模预算的哈希算法实现. 此算法采用两次哈希算法进行数据库表的定位，第一次哈希选择数据库，第二次哈希选择数据库表.
 * 两次哈希使用的同一个分库分表因子，因此在使用此算法时需要注意一点，分库的数量和分表的数量需要具有奇偶性
 * 当数据库数量为奇数时每个库的分表个数必须是偶数，否则数据不会被均匀的散列在表中.
 * 
 * @author duanbn
 */
public class SimpleHashClusterRouterImpl extends AbstractDBRouterImpl {

	@Override
	public DBRouteInfo doSelectFromMaster(Map<String, List<DBClusterInfo>> dbMasterCluster, IShardingValue<?> value)
			throws DBRouteException {
		DBRouteInfo dbRoute = new DBRouteInfo();

		String clusterName = value.getClusterName();
		List<DBClusterInfo> dbClusters = dbMasterCluster.get(clusterName);
		if (dbClusters == null || dbClusters.isEmpty()) {
			throw new DBRouteException("查找主库集群失败, dbname=" + clusterName);
		}

		long shardingValue = getShardingValue(value);
		DBClusterInfo rangeClusterInfo = null;
		int clusterIndex = 0;
		for (DBClusterInfo dbClusterInfo : dbClusters) {
			if (dbClusterInfo.getStart() <= shardingValue && dbClusterInfo.getEnd() >= shardingValue) {
				rangeClusterInfo = dbClusterInfo;
				break;
			}
			clusterIndex++;
		}
		if (rangeClusterInfo == null) {
			throw new DBRouteException("查找集群失败, 超出容量, dbname=" + clusterName + ", shardingvalue=" + shardingValue);
		}

		int dbNum = rangeClusterInfo.getDbConnInfos().size();
		int dbIndex = (int) shardingValue % dbNum;

		dbRoute.setClusterIndex(clusterIndex);
		dbRoute.setClusterName(clusterName);
		dbRoute.setDbIndex(dbIndex);

		return dbRoute;
	}

	@Override
	public DBRouteInfo doSelectFromSlave(Map<String, List<List<DBClusterInfo>>> dbSlaveCluster, int slaveIndex,
			IShardingValue<?> value) throws DBRouteException {
		DBRouteInfo dbRoute = new DBRouteInfo();

		String clusterName = value.getClusterName();
		List<List<DBClusterInfo>> slaveCluster = dbSlaveCluster.get(clusterName);
		if (slaveCluster == null || slaveCluster.isEmpty()) {
			throw new DBRouteException("查找从库集群失败, dbname=" + clusterName);
		}

		List<DBClusterInfo> dbClusters = slaveCluster.get(slaveIndex);
		if (dbClusters == null || dbClusters.isEmpty()) {
			throw new DBRouteException("查找从库集群失败, dbname=" + clusterName + ", slaveindex=" + slaveIndex);
		}

		long shardingValue = getShardingValue(value);
		DBClusterInfo rangeClusterInfo = null;
		int clusterIndex = 0;
		for (DBClusterInfo dbClusterInfo : dbClusters) {
			if (dbClusterInfo.getStart() <= shardingValue && dbClusterInfo.getEnd() >= shardingValue) {
				rangeClusterInfo = dbClusterInfo;
				break;
			}
			clusterIndex++;
		}
		if (rangeClusterInfo == null) {
			throw new DBRouteException("查找集群失败, 超出容量, dbname=" + clusterName + ", shardingvalue=" + shardingValue);
		}

		int dbNum = rangeClusterInfo.getDbConnInfos().size();

		int dbIndex = (int) shardingValue % dbNum;

		dbRoute.setClusterIndex(clusterIndex);
		dbRoute.setClusterName(clusterName);
		dbRoute.setDbIndex(dbIndex);

		return dbRoute;
	}

}
