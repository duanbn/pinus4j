package com.pinus.cluster.route;

import java.util.List;
import java.util.Map;

import com.pinus.api.IShardingKey;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.enums.HashAlgoEnum;
import com.pinus.exception.DBRouteException;

/**
 * 数据库集群路由器.
 * 负责依据ShardingValue的值来找到合适库表. 不同的路由算法都必须实现此接口.
 * 三种配置信息获取方式，1. 从classpath根路径的storage-config.properties中获取。
 * 2. 从指定的文件中获取。 3. 从zookeeper中获取.
 * 优先从zookeeper中加载，其次从指定的文件，默认从classpath根路径
 *
 * @author duanbn
 */
public interface IClusterRouter {
	
	/**
	 * 设置hash算法
	 * @param algoEnum
	 */
	public void setHashAlgo(HashAlgoEnum algoEnum);
	
	/**
	 * 获取hash算法
	 * @return
	 */
	public HashAlgoEnum getHashAlgo();

    /**
     * 设置主库集群.
     *
     * @param masterDbClusterInfo 主库集群信息.
     */
    public void setMasterDbClusterInfo(Map<String, List<DBClusterInfo>> masterDbClusterInfo);

    /**
     * 设置从库集群.
     * 
     * @param slaveDbClusterInfo 从库集群信息.
     */
    public void setSlaveDbClusterInfo(Map<String, List<List<DBClusterInfo>>> slaveDbClusterInfo);

    /**
     * 设置数据表集群.
     *
     * @param tableCluster 数据表信息{集群名称, {分库下标, {表名, 分表数}}}
     */
    public void setTableCluster(Map<String, Map<Integer, Map<String, Integer>>> tableCluster);

    /**
     * 获取数据表集群.
     *
     * @return 数据表集群信息{集群名称, {分库下标, {表名, 分表数}}}
     */
    public Map<String, Map<Integer, Map<String, Integer>>> getTableCluster();

    /**
     * 选择需要操作的数据库表.
     *
     * @param clusterType 主从库类型.
     * @param tableName 表名.
     * @param value 分库分表因子.
     *
     * @return 命中的分库分表信息.
     *
     * @throws DBRouteException 路由操作失败
     */
    public DBRouteInfo select(EnumDBMasterSlave clusterType, String tableName, IShardingKey<?> value) throws DBRouteException;

}
