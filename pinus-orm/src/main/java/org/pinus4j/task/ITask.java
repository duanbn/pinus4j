/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.task;

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
	 * 一次批量读取记录. <b>此方法会在多线程环境下执行</b>
	 * 
	 * @param entity
	 *            记录
	 */
	public void batchRecord(List<T> entity);
	
	/**
	 * 一次批量读取之后执行此方法.
	 */
	public void afterBatch();
	
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
