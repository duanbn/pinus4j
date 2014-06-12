package com.pinus.datalayer;

import com.pinus.api.query.IQuery;
import com.pinus.datalayer.beans.DBClusterIteratorInfo;

/**
 * 遍历集群数据接口. 此接口设计用来在集群中对单个数据对象进行遍历。
 * 
 * @author duanbn
 * @since 0.6.0
 */
public interface IShardingIterator<E> {

	/**
	 * 设置查询条件
	 * 
	 * @param query
	 */
	public void setQuery(IQuery query);

	/**
	 * 是否有下一个元素.
	 * 
	 * @return true:有, false:无
	 */
	public boolean hasNext();

	/**
	 * 获取下一个元素.
	 * 
	 * @return 被遍历的元素
	 */
	public E next();

	/**
	 * 获取当前遍历的数据库下标
	 * 
	 * @return 正在遍历的数据库下标
	 */
	public int curDbIndex();

	/**
	 * 获取当前遍历的表下标.
	 * 
	 * @return 正在遍历表下标
	 */
	public int curTableIndex();

	/**
	 * 获取当前遍历的数据id.
	 */
	public long curEntityId();

	/**
	 * 获取当前遍历器的遍历信息.
	 */
	public DBClusterIteratorInfo curIteratorInfo();

}
