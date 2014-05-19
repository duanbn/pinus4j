package com.pinus.datalayer.beans;

import java.util.HashMap;
import java.util.Map;

import com.pinus.cluster.DB;

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
