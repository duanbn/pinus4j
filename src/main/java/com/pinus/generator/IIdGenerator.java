package com.pinus.generator;

import com.pinus.cluster.IDBCluster;
import com.pinus.config.IClusterConfig;
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
	public int genClusterUniqueIntId(IDBCluster dbCluster, String clusterName, String name);

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
	public long genClusterUniqueLongId(IDBCluster dbCluster, String clusterName, String name);

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
	public long[] genClusterUniqueLongIdBatch(IDBCluster dbCluster, String clusterName, String name, int batchSize);

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
	public int[] genClusterUniqueIntIdBatch(IDBCluster dbCluster, String clusterName, String name, int batchSize);

}
