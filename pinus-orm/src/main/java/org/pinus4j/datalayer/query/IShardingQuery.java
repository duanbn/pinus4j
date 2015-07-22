package org.pinus4j.datalayer.query;

import java.util.List;
import java.util.Map;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;

/**
 * query sharding data.
 * 
 * @author duanbn
 * @since 1.1.1
 *
 */
public interface IShardingQuery {

	Number getCount(Class<?> clazz, boolean useCache);

	Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache);

	Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache);

	Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache);

	Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> T findByPk(Number pk, Class<T> clazz, boolean useCache);

	<T> T findByPk(Number pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, boolean useCache);

	<T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> List<T> findByPkList(List<? extends Number> pkList, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache);

	<T> List<T> findByPkList(List<? extends Number> pkList, IShardingKey<?> shardingKey, Class<T> clazz,
			boolean useCache, EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache);

	<T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache);

	<T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

	<T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache);

	<T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
			EnumDBMasterSlave masterSlave);

	List<Map<String, Object>> findBySql(SQL sql);

	List<Map<String, Object>> findBySql(SQL sql, EnumDBMasterSlave masterSlave);

	List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey);

	List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave);

}
