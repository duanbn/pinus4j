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

package org.pinus4j.datalayer;

import java.util.Iterator;
import java.util.List;

import org.pinus4j.api.query.IQuery;

/**
 * 记录遍历器.
 * 
 * @author duanbn
 *
 * @param <E>
 */
public interface IRecordIterator<E> extends Iterator<E> {

	/**
	 * 批量返回
	 * 
	 * @return
	 */
	public List<E> nextMore();

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

	/**
	 * 设置批量读取记录的条数
	 * 
	 * @param step
	 */
	public void setStep(int step);

}
