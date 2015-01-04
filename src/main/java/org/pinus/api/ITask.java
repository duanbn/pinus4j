package org.pinus.api;

import java.util.List;

/**
 * 数据处理任务. 一次处理任务只会存在一个task对象.
 * 
 * @author duanbn
 *
 */
public interface ITask<T> {

	/**
	 * 任务开始时会调用此方法
	 */
	public void init() throws Exception;

	/**
	 * 处理数据. <b>此方法会在多线程环境下执行</b>
	 * 
	 * @param entity
	 *            一条数据库记录.
	 */
	public void doTask(List<T> entity);

	/**
	 * 本次任务完成时会调用此方法.
	 */
	public void finish() throws Exception;

	/**
	 * 设置批处理一次读取的记录条数
	 * 
	 * @return
	 */
	public int taskBuffer();

}
