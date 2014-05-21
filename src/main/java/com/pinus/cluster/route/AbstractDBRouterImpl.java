package com.pinus.cluster.route;

import java.util.List;
import java.util.Map;

import com.pinus.api.IShardingValue;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBClusterInfo;
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
	private Map<String, List<DBClusterInfo>> dbMasterCluster;
	/**
	 * 从库集群.
	 */
	private Map<String, List<List<DBClusterInfo>>> dbSlaveCluster;

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
	public void setMasterDbClusterInfo(Map<String, List<DBClusterInfo>> masterDbClusterInfo) {
		this.dbMasterCluster = masterDbClusterInfo;
	}

	@Override
	public void setSlaveDbClusterInfo(Map<String, List<List<DBClusterInfo>>> slaveDbClusterInfo) {
		this.dbSlaveCluster = slaveDbClusterInfo;
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
	public DBRouteInfo select(EnumDBMasterSlave clusterType, String tableName, IShardingValue<?> value)
			throws DBRouteException {
		DBRouteInfo dbRouteInfo = null;

		switch (clusterType) {
		case MASTER:
			dbRouteInfo = doSelectFromMaster(this.dbMasterCluster, value);
			break;
		default:
			int slaveIndex = clusterType.getValue();
			dbRouteInfo = doSelectFromSlave(this.dbSlaveCluster, slaveIndex, value);
			break;
		}

		if (dbRouteInfo == null) {
			throw new RuntimeException("路由操作失败， 找不到相关的库表, clusterType=" + clusterType + ", sharding value=" + value);
		}

		// 计算分表.
		try {
			Map<Integer, Map<String, Integer>> dbCluster = tableCluster.get(dbRouteInfo.getClusterName());
			Map<String, Integer> tableCluster = dbCluster.get(dbRouteInfo.getDbIndex());
			int tableNum = tableCluster.get(tableName);

			// 计算分表下标
			int tableIndex = (int) getShardingValue(value) % tableNum;

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
	protected long getShardingValue(IShardingValue<?> value) {
		Object shardingValue = value.getShardingValue();

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
	protected abstract DBRouteInfo doSelectFromMaster(Map<String, List<DBClusterInfo>> dbMasterCluster,
			IShardingValue<?> value) throws DBRouteException;

	/**
	 * 路由操作. 从从库中获取路由库表.
	 * 
	 * @param dbSlaveCluster
	 *            从库集群信息
	 * @param value
	 *            分库分表因子
	 * @return 路由结果
	 */
	protected abstract DBRouteInfo doSelectFromSlave(Map<String, List<List<DBClusterInfo>>> dbSlaveCluster,
			int slaveIndex, IShardingValue<?> value) throws DBRouteException;

}
