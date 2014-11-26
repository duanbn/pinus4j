package com.pinus.cache;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.pinus.cluster.DB;

/**
 * 主缓存接口. 为Pinus存储提供一级缓存, 一级缓存使用memcached作为存储主要是对数据库表中的数据进行缓存查询进行缓存.
 * 一级缓存的key格式：[clusterName + dbIndex].[tableName + tableIndex].id, value是数据库记录.
 * 
 * @author duanbn
 */
public interface IPrimaryCache {

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
	 * 设置count数.
	 * 
	 * @param db
	 *            分库分表
	 * @param count
	 *            count数
	 */
	public void setCountGlobal(String clusterName, String tableName, long count);

	/**
	 * 删除count数.
	 * 
	 * @param db
	 *            分库分表
	 */
	public void removeCountGlobal(String clusterName, String tableName);

	/**
	 * 减少分表count数.
	 * 
	 * @param db
	 *            分库分表
	 * @param delta
	 *            减少数
	 * 
	 * @return 减少后的count数
	 */
	public long decrCountGlobal(String clusterName, String tableName, int delta);

	/**
	 * 增加分表count数.
	 * 
	 * @param db
	 *            分库分表
	 * @param delta
	 *            增加数
	 * 
	 * @return 增加后的count数
	 */
	public long incrCountGlobal(String clusterName, String tableName, int delta);

	/**
	 * 获取一张表的count值.
	 * 
	 * @param db
	 *            分库分表
	 * 
	 * @return count值.
	 */
	public long getCountGlobal(String clusterName, String tableName);

	/**
	 * 添加一条记录. 如果存在则替换.
	 * 
	 * @param db
	 *            分库分表
	 * @param id
	 *            主键
	 * @param data
	 *            记录
	 */
	public void putGlobal(String clusterName, String tableName, Number id, Object data);

	/**
	 * 批量添加记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param ids
	 *            主键
	 * @param data
	 *            批量数据
	 */
	public void putGlobal(String clusterName, String tableName, List<? extends Object> data);

	/**
	 * 批量添加记录
	 * 
	 * @param clusterName
	 * @param tableName
	 * @param data
	 */
	public void putGlobal(String clusterName, String tableName, Map<Number, ? extends Object> data);

	/**
	 * 获取记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param id
	 *            主键
	 * 
	 * @return 记录
	 */
	public <T> T getGlobal(String clusterName, String tableName, Number Id);

	/**
	 * 获取多条记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param ids
	 *            主键
	 * 
	 * @return 多条数据
	 */
	public <T> List<T> getGlobal(String clusterName, String tableName, Number[] ids);

	/**
	 * 删除一条记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param id
	 *            主键
	 */
	public void removeGlobal(String clusterName, String tableName, Number id);

	/**
	 * 批量删除缓存.
	 * 
	 * @param db
	 *            分库分表
	 * @param ids
	 *            主键
	 */
	public void removeGlobal(String clusterName, String tableName, List<? extends Number> ids);

	/**
	 * 设置count数.
	 * 
	 * @param db
	 *            分库分表
	 * @param count
	 *            count数
	 */
	public void setCount(DB db, long count);

	/**
	 * 删除count数.
	 * 
	 * @param db
	 *            分库分表
	 */
	public void removeCount(DB db);

	/**
	 * 减少分表count数.
	 * 
	 * @param db
	 *            分库分表
	 * @param delta
	 *            减少数
	 * 
	 * @return 减少后的count数
	 */
	public long decrCount(DB db, long delta);

	/**
	 * 增加分表count数.
	 * 
	 * @param db
	 *            分库分表
	 * @param delta
	 *            增加数
	 * 
	 * @return 增加后的count数
	 */
	public long incrCount(DB db, long delta);

	/**
	 * 获取一张表的count值.
	 * 
	 * @param db
	 *            分库分表
	 * 
	 * @return count值.
	 */
	public long getCount(DB db);

	/**
	 * 添加一条记录. 如果存在则替换.
	 * 
	 * @param db
	 *            分库分表
	 * @param id
	 *            主键
	 * @param data
	 *            记录
	 */
	public void put(DB db, Number id, Object data);

	/**
	 * 批量添加记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param ids
	 *            主键
	 * @param data
	 *            批量数据
	 */
	public void put(DB db, Number[] ids, List<? extends Object> data);

	/**
	 * 批量添加记录.
	 * 
	 * @param db
	 * @param data
	 */
	public void put(DB db, Map<Number, ? extends Object> data);

	/**
	 * 获取记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param id
	 *            主键
	 * 
	 * @return 记录
	 */
	public <T> T get(DB db, Number Id);

	/**
	 * 获取多条记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param ids
	 *            主键
	 * 
	 * @return 多条数据
	 */
	public <T> List<T> get(DB db, Number... ids);

	/**
	 * 删除一条记录.
	 * 
	 * @param db
	 *            分库分表
	 * @param id
	 *            主键
	 */
	public void remove(DB db, Number pk);

	/**
	 * 批量删除缓存.
	 * 
	 * @param db
	 *            分库分表
	 * @param ids
	 *            主键
	 */
	public void remove(DB db, List<? extends Number> pks);

}
