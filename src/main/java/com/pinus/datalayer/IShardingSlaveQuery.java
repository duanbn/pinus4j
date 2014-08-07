package com.pinus.datalayer;

import java.util.List;

import com.pinus.api.IShardingKey;
import com.pinus.api.SQL;
import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.api.query.IQuery;
import com.pinus.cache.IPrimaryCache;
import com.pinus.cluster.IDBCluster;
import com.pinus.exception.DBOperationException;

/**
 * 从库查询操作接口.
 * 
 * @author duanbn
 */
public interface IShardingSlaveQuery {

	public Number getGlobalCountFromSlave(String clusterName, Class<?> clazz, EnumDBMasterSlave slave);

	public <T> T findGlobalByPkFromSlave(Number pk, String clusterName, Class<T> clazz, EnumDBMasterSlave slave);

	public <T> T findGlobalOneByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz, EnumDBMasterSlave slave);

	public <T> List<T> findGlobalByPksFromSlave(String clusterName, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks);

	public <T> List<T> findGlobalByPksFromSlave(List<? extends Number> pks, String clusterName, Class<T> clazz,
			EnumDBMasterSlave slave);

	public <T> List<T> findGlobalBySqlFromSlave(SQL<T> sql, String clusterName, EnumDBMasterSlave slave);

	public <T> List<T> findGlobalByQueryFromSlave(IQuery query, String clusterName, Class<T> clazz,
			EnumDBMasterSlave slave);

	/**
	 * 设置数据库集群.
	 */
	public void setDBCluster(IDBCluster dbCluster);

	/**
	 * 获取从分库分表记录总数.
	 * 
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象
	 * 
	 * @return 表记录总数
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public Number getCountFromSlave(IShardingKey<?> shardingValue, Class<?> clazz, EnumDBMasterSlave slave);

	/**
	 * 一个从分库分表, 根据主键查询.
	 * 
	 * @param pk
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> T findByPkFromSlave(Number pk, IShardingKey<?> shardingValue, Class<T> clazz, EnumDBMasterSlave slave);

	/**
	 * 根据查询条件获取一条数据. 如果查询到多条则返回第一条.
	 * 
	 * @param query
	 * @param shardingValue
	 * @param clazz
	 * @param slave
	 * @return 查询结果，找不到返回null
	 */
	public <T> T findOneByQueryFromSlave(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz,
			EnumDBMasterSlave slave);

	/**
	 * 一个从分库分表, 根据多个主键查询.
	 * 
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * @param pks
	 *            主键
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByPksFromSlave(IShardingKey<?> shardingValue, Class<T> clazz, EnumDBMasterSlave slave,
			Number... pks);

	/**
	 * 一个从分库分表, 根据多个主键查询.
	 * 
	 * @param pks
	 *            主键
	 * @param shardingValue
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByPkListFromSlave(List<? extends Number> pks, IShardingKey<?> shardingValue, Class<T> clazz,
			EnumDBMasterSlave slave);

	/**
	 * 多个从分库分表, 根据多个主键查询.
	 * 
	 * @param shardingValues
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * @param pks
	 *            主键
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByShardingPairFromSlave(List<IShardingKey<?>> shardingValues, Class<T> clazz,
			EnumDBMasterSlave slave, Number... pks);

	/**
	 * 多个从分库分表, 根据多个主键查询.
	 * 
	 * @param pks
	 *            主键
	 * @param shardingValues
	 *            分库分表因子
	 * @param clazz
	 *            数据对象类型
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByShardingPairFromSlave(List<Number> pks, List<IShardingKey<?>> shardingValues,
			Class<T> clazz, EnumDBMasterSlave slave);

	/**
	 * 一个从分库分表, 根据条件查询.
	 * 
	 * @param sql
	 *            查询语句
	 * @param shardingValue
	 *            分库分表因子
	 * @param slave
	 *            主从库枚举
	 * 
	 * @return 查询结果
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findBySqlFromSlave(SQL<T> sql, IShardingKey<?> shardingValue, EnumDBMasterSlave slave);

	/**
	 * 根据查询条件对象进行查询.
	 * 
	 * @param query
	 *            查询条件
	 * @param shardingValue
	 *            分库分表因子
	 * @param slave
	 *            从库查询
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 * @throws IllegalArgumentException
	 *             输入参数错误
	 */
	public <T> List<T> findByQueryFromSlave(IQuery query, IShardingKey<?> shardingValue, Class<T> clazz,
			EnumDBMasterSlave slave);

	/**
	 * 设置缓存.
	 */
	public void setPrimaryCache(IPrimaryCache primaryCache);

}
