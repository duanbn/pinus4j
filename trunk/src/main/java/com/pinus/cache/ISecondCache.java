package com.pinus.cache;

/**
 * 二级缓存接口.
 * 二级缓存提供对条件查询的结果进行缓存.
 * 二级缓存的key格式：[clusterName + dbIndex].[tableName + tableIndex].[query condition]
 *
 * @author duanbn
 */
public interface ISecondCache {
}
