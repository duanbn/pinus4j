package com.pinus.api;

/**
 * 基于数值的分库分表路由因子接口实现. 集群路由器会根据此对象的value值计算分库分表.
 * 
 * @author duanbn
 */
public class ShardingValue<T> implements IShardingValue<T> {

	/**
	 * 集群数据库名称.
	 */
	private String clusterName;

	/**
	 * 分库分表因子.
	 */
	private T value;

	/**
	 * 构造方法.
	 * 
	 * @param clusterName
	 *            数据库集群名
	 */
	public ShardingValue(String clusterName) {
		this(clusterName, null);
	}

	/**
	 * 构造方法.
	 * 
	 * @param clusterName
	 *            数据库集群名
	 * @param value
	 *            分库分表因子值
	 */
	public ShardingValue(String clusterName, T value) {
		this.clusterName = clusterName;
		this.value = value;
	}

	@Override
	public String getClusterName() {
		return this.clusterName;
	}

	@Override
	public T getShardingValue() {
		return this.value;
	}

	@Override
	public String toString() {
		StringBuilder info = new StringBuilder();
		info.append("clusterName=").append(this.clusterName);
		info.append(", value=").append(this.value);
		return info.toString();
	}

}
