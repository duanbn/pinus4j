package com.pinus.api;

/**
 * 分库分表路由因子接口. 中间件将通过这个值进行库表的路由，找到本次操作需要读或者写得库表.
 * <b>目前分库分表因子的值只能是String或者Number类型</b>
 * 
 * @author duanbn
 */
public interface IShardingValue<T> {

	/**
	 * 获取集群名称.
	 */
	public String getClusterName();

	/**
	 * 获取分库分表因子.
	 */
	public T getShardingValue();

}
