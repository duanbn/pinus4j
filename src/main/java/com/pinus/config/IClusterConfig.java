package com.pinus.config;

import java.util.Map;

import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.enums.HashAlgoEnum;

/**
 * 存储中间件配置信息接口. 此接口提供的信息都是通过配置来获取.
 * 一个storage-config.properties中可以配置多个数据库集群，每个数据库集群又可以配置一个主库集群和多个从库集群.
 * 
 * @author duanbn
 */
public interface IClusterConfig {

	/**
	 * 获取ID生成器默认批量生成值.
	 * 
	 * @return
	 */
	public int getIdGeneratorBatch();

	/**
	 * 获取zookeeper的连接地址
	 */
	public String getZkUrl();

	/**
	 * 获取配置的hash算法.
	 * 
	 * @return hash算法枚举
	 */
	public HashAlgoEnum getHashAlgo();

	/**
	 * 加载主全局库.
	 * 
	 * @return {集群名, 连接信息}
	 */
	public Map<String, DBConnectionInfo> loadMasterGlobalInfo();

	/**
	 * 加载从全局库.
	 * 
	 * @return {集群名, {从库号, 连接信息}}
	 */
	public Map<String, Map<Integer, DBConnectionInfo>> loadSlaveGlobalInfo();

	/**
	 * 加载主库集群信息.
	 * 
	 * @return 主库集群信息, key:数据库名, value:集群信息.
	 */
	public Map<String, DBClusterInfo> loadMasterDbClusterInfo();

	/**
	 * 加载从库集群信息.
	 * 
	 * @return 从库集群信息, key:数据库名, value:从库信息key:从库号, value集群信息
	 */
	public Map<String, Map<Integer, DBClusterInfo>> loadSlaveDbClusterInfo();

}
