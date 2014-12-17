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

package org.pinus.datalayer.beans;

import java.util.HashMap;
import java.util.Map;

import org.pinus.cluster.DB;

/**
 * 集群统计信息.
 * 
 * @author duanbn
 * 
 */
public class DBClusterStatInfo {

	Map<DB, Integer> shardingEntityCount = new HashMap<DB, Integer>();

	/**
	 * 获取统计信息的文本描述.
	 * 
	 * @return 统计信息文本描述
	 */
	public String getText() {
		StringBuilder text = new StringBuilder();
		long totle = 0;
		for (Map.Entry<DB, Integer> countMap : shardingEntityCount.entrySet()) {
			text.append(countMap.getKey()).append(" - ").append(countMap.getValue()).append("\n");
			totle += countMap.getValue();
		}
		text.append("总数:" + totle);
		return text.toString();
	}

	public Map<DB, Integer> getShardingEntityCount() {
		return shardingEntityCount;
	}

	public void setShardingEntityCount(Map<DB, Integer> shardingEntityCount) {
		this.shardingEntityCount = shardingEntityCount;
	}

}
