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

package org.pinus.cluster.route.impl;

import java.util.List;

import org.pinus.api.IShardingKey;
import org.pinus.cluster.beans.DBInfo;
import org.pinus.exception.DBRouteException;

/**
 * default simple hash algo router implements.
 * 
 * @author duanbn
 * @since 0.1.0
 */
public class SimpleHashClusterRouter extends AbstractClusterRouter {

	@Override
	public DBInfo doSelect(List<DBInfo> dbInfos, IShardingKey<?> value) throws DBRouteException {

		long shardingValue = getShardingValue(value);

		int dbNum = dbInfos.size();

		int dbIndex = (int) shardingValue % dbNum;

		return dbInfos.get(dbIndex);
	}

}
