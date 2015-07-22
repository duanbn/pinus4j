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

package org.pinus4j.cluster.beans;

/**
 * 分库分表路由因子接口. 中间件将通过这个值进行库表的路由，找到本次操作需要读或者写得库表.
 * <b>目前分库分表因子的值只能是String或者Number类型</b>
 * 
 * @author duanbn
 */
public interface IShardingKey<T> {

	/**
	 * 获取集群名称.
	 */
	public String getClusterName();

	/**
	 * 获取分库分表因子.
	 */
	public T getValue();
	
    /**
     * 设置分库分表因子.
     */
	public void setValue(T value);

}
