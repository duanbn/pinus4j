package com.pinus.api;

/**
 * 需要进行分库分表的数据对象可以实现此接口. 此接口和IShardingValue接口的方法一致，但是此接口主要是用于写入数据.
 * 
 * @author duanbn
 * 
 */
public interface IShardingEntity<T> {

	/**
	 * 获取集群名称.
	 */
	public String getClusterName();

	/**
	 * 获取分库分表因子.
	 */
	public T getShardingValue();

}
