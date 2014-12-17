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

package com.pinus.cluster.route.impl;

import java.util.List;

import com.pinus.api.IShardingKey;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.route.AbstractDBRouterImpl;
import com.pinus.cluster.route.DBRouteInfo;
import com.pinus.exception.DBRouteException;

/**
 * 基于取模预算的哈希算法实现. 此算法采用两次哈希算法进行数据库表的定位，第一次哈希选择数据库，第二次哈希选择数据库表.
 * 两次哈希使用的同一个分库分表因子，因此在使用此算法时需要注意一点，分库的数量和分表的数量需要具有奇偶性
 * 当数据库数量为奇数时每个库的分表个数必须是偶数，否则数据不会被均匀的散列在表中.
 * 
 * @author duanbn
 */
public class SimpleHashClusterRouterImpl extends AbstractDBRouterImpl {

	@Override
	public DBRouteInfo doSelectFromMaster(List<DBConnectionInfo> masterConnection, IShardingKey<?> value)
			throws DBRouteException {
		DBRouteInfo dbRoute = new DBRouteInfo();

		long shardingValue = getShardingValue(value);
		int dbNum = masterConnection.size();
		int dbIndex = (int) shardingValue % dbNum;

		dbRoute.setDbIndex(dbIndex);

		return dbRoute;
	}

	@Override
	public DBRouteInfo doSelectFromSlave(List<List<DBConnectionInfo>> slaveConnections, int slaveIndex,
			IShardingKey<?> value) throws DBRouteException {
		DBRouteInfo dbRoute = new DBRouteInfo();

		List<DBConnectionInfo> slaveConnection = slaveConnections.get(slaveIndex);
		if (slaveConnection == null || slaveConnection.isEmpty()) {
			throw new DBRouteException("查找从库集群失败, dbname=" + value.getClusterName() + ", slaveindex=" + slaveIndex);
		}

		long shardingValue = getShardingValue(value);
		int dbNum = slaveConnection.size();

		int dbIndex = (int) shardingValue % dbNum;

		dbRoute.setDbIndex(dbIndex);

		return dbRoute;
	}

}
