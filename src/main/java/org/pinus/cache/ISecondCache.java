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

package org.pinus.cache;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;

import org.pinus.api.query.IQuery;
import org.pinus.cluster.DB;

/**
 * 二级缓存接口. 二级缓存提供对条件查询的结果进行缓存. 二级缓存的key格式：[clusterName + dbIndex].[tableName +
 * tableIndex].[query condition]
 *
 * @author duanbn
 */
public interface ISecondCache {

	/**
	 * 获取过期时间
	 * 
	 * @return
	 */
	public int getExpire();

	/**
	 * 销毁对象
	 */
	public void destroy();

	/**
	 * 获取可以用的服务链接.
	 */
	public Collection<SocketAddress> getAvailableServers();

	/**
	 * 添加到全局缓存
	 *
	 * @param query
	 *            查询条件
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            对象class
	 */
	public void putGlobal(IQuery query, String clusterName, String tableName, List data);

	/**
	 * 读取全局缓存
	 *
	 * @param query
	 *            查询条件
	 * @param clusterName
	 *            集群名
	 * @param clazz
	 *            对象class
	 *
	 * @return 结果集
	 */
	public List getGlobal(IQuery query, String clusterName, String tableName);

	/**
	 * 清除全局缓存.
	 */
	public void removeGlobal(String clusterName, String tableName);

	/**
	 * 添加到分片缓存.
	 *
	 * @param query
	 *            查询条件
	 * @param data
	 *            结果集
	 */
	public void put(IQuery query, DB db, List data);

	/**
	 * 读取分片缓存.
	 *
	 * @param query
	 *            查询条件
	 *
	 * @return 结果集.
	 */
	public List get(IQuery query, DB db);

	/**
	 * 清除分片缓存.
	 *
	 * @param query
	 *            查询条件.
	 */
	public void remove(DB db);

}
