package com.pinus.cluster.route.impl;

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
	public DBRouteInfo doSelectFromMaster(Map<String, DBClusterInfo> dbMasterCluster, IShardingValue<?> value)
			throws DBRouteException {
		DBRouteInfo dbRoute = new DBRouteInfo();

		String clusterName = value.getClusterName();
		DBClusterInfo dbCluster = dbMasterCluster.get(clusterName);
		if (dbCluster == null) {
			throw new DBRouteException("查找主库集群失败, dbname=" + clusterName);
		}
		int dbNum = dbCluster.getDbConnInfos().size();
		int dbIndex = computeMod(value, dbNum);

		dbRoute.setClusterName(clusterName);
		dbRoute.setDbIndex(dbIndex);

		return dbRoute;
	}

	@Override
	public DBRouteInfo doSelectFromSlave(Map<String, Map<Integer, DBClusterInfo>> dbSlaveCluster, int slaveIndex,
			IShardingValue<?> value) throws DBRouteException {
		DBRouteInfo dbRoute = new DBRouteInfo();

		String clusterName = value.getClusterName();
		Map<Integer, DBClusterInfo> slaveCluster = null;
		DBClusterInfo dbCluster = null;
		try {
			slaveCluster = dbSlaveCluster.get(clusterName);
			dbCluster = slaveCluster.get(slaveIndex);
		} catch (Exception e) {
			throw new DBRouteException("查找从库集群失败, dbname=" + clusterName + ", slaveindex=" + slaveIndex);
		}
		int dbNum = dbCluster.getDbConnInfos().size();
		int dbIndex = computeMod(value, dbNum);

		dbRoute.setClusterName(clusterName);
		dbRoute.setDbIndex(dbIndex);

		return dbRoute;
	}

}
