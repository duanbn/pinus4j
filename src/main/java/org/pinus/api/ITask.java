package org.pinus.api;

/**
 * 数据处理任务.
 * 
 * @author duanbn
 *
 */
public interface ITask<T> {

	/**
	 * 处理数据
	 * 
	 * @param entity
	 *            一条数据库记录.
	 */
	public void doTask(T entity);

}
