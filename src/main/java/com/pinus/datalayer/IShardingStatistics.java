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
