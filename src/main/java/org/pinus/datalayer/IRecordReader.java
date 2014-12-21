package org.pinus.datalayer;

import java.util.Iterator;

import org.pinus.api.query.IQuery;

/**
 * 记录遍历器.
 * 
 * @author duanbn
 *
 * @param <E>
 */
public interface IRecordReader<E> extends Iterator<E> {

	/**
	 * 获取此遍历器需要遍历的结果集总数.
	 * 
	 * @return
	 */
	public long getCount();

	/**
	 * 设置遍历时查询的条件
	 * 
	 * @param query
	 */
	public void setQuery(IQuery query);

}
