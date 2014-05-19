package com.pinus.api;

/**
 * 需要保存在全局表里的数据对象可以实现此接口.
 * 
 * @author duanbn
 * 
 */
public interface IGlobalEntity {

	/**
	 * 获取集群名称.
	 */
	public String getClusterName();

}
