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

package org.pinus.util;

import java.util.List;

import org.pinus.api.IShardingKey;
import org.pinus.api.SQL;
import org.pinus.api.annotation.Table;
import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.api.query.IQuery;
import org.pinus.exception.DBOperationException;

/**
 * 校验工具.
 * 
 * @author duanbn
 */
public class CheckUtil {

	/**
	 * 校验分页参数.
	 * 
	 * @param start
	 * @param limit
	 */
	public static void checkLimit(int start, int limit) {
		if (start < 0 || limit <= 0) {
			throw new IllegalArgumentException("分页参数错误, start=" + start + ", limit=" + limit);
		}
	}

	/**
	 * 校验集群名称.
	 * 
	 * @param clusterName
	 */
	public static void checkClusterName(String clusterName) {
		if (StringUtils.isBlank(clusterName)) {
			throw new IllegalArgumentException("参数错误, clusterName不能为空");
		}
	}

	/**
	 * 校验EnumDBMasterSlave对象.
	 */
	public static void checkEnumMasterSlave(EnumDBMasterSlave clusterType) {
		if (clusterType == null) {
			throw new IllegalArgumentException("参数错误, EnumDBMasterSlave=null");
		}
	}

	/**
	 * 校验Query对象.
	 */
	public static void checkQuery(IQuery query) {
		if (query == null) {
			throw new IllegalArgumentException("参数错误, Query=" + query);
		}
	}

	/**
	 * 校验SQL对象.
	 * 
	 * @param sql
	 *            SQL对象
	 */
	public static void checkSQL(SQL sql) {
		if (sql == null) {
			throw new IllegalArgumentException("参数错误, SQL=" + sql);
		}

		if (sql.getSql() == null || sql.getSql().equals("")) {
			throw new IllegalArgumentException("参数错误, SQL的sql语句为空");
		}
	}

	/**
	 * 校验数据必须大于0
	 * 
	 * @param number
	 * 
	 * @throws IllegalArgumentException
	 *             校验失败
	 */
	public static void checkNumberGtZero(Number number) {
		if (number == null) {
			throw new IllegalArgumentException("参数错误, number=" + number);
		}

		if (number.intValue() <= 0) {
			throw new IllegalArgumentException("参数错误, number=" + number);
		}
	}

	/**
	 * 校验列表参数.
	 * 
	 * @param numbers
	 *            List<Number>
	 * 
	 * @throws IllegalArgumentException
	 *             校验失败
	 */
	public static void checkNumberList(List<? extends Number> numbers) {
		if (numbers == null) {
			throw new IllegalArgumentException("参数错误, list=" + numbers);
		}
	}

	public static void checkEntityList(List<? extends Object> entityList) {
		if (entityList == null || entityList.isEmpty()) {
			throw new IllegalArgumentException("参数错误, entity list=" + entityList);
		}
	}

	/**
	 * 校验分库分表因子.
	 * 
	 * @param shardingValue
	 *            IShardingValue<?>
	 * 
	 * @throws IllegalArgumentException
	 *             校验失败
	 */
	public static void checkShardingValue(IShardingKey<?> shardingKey) {
		if (shardingKey == null || StringUtils.isBlank(shardingKey.getClusterName())) {
			throw new IllegalArgumentException("参数错误, shardingKey=" + shardingKey);
		}
		if (shardingKey.getValue() instanceof Number) {
			if (shardingKey.getValue() == null || ((Number) shardingKey.getValue()).intValue() == 0) {
				throw new DBOperationException(
						"sharding value cann't be null or number zero when sharding value type is number");
			}
		} else if (shardingKey.getValue() instanceof String) {
			if (shardingKey.getValue() == null) {
				throw new DBOperationException("使用String做Sharding时，ShardingKey的值不能为Null");
			}
		} else {
			throw new DBOperationException("不支持的ShardingKey类型, 只支持Number或String");
		}
	}

	/**
	 * 校验分库分表因子列表参数.
	 */
	public static void checkShardingValueList(List<IShardingKey<?>> shardingValueList) {
		if (shardingValueList == null || shardingValueList.isEmpty()) {
			throw new IllegalArgumentException("参数错误, sharding value list=" + shardingValueList);
		}
	}

	/**
	 * 校验Class
	 * 
	 * @param clazz
	 *            Class<?>
	 * 
	 * @throws IllegalArgumentException
	 *             校验失败
	 */
	public static void checkClass(Class<?> clazz) {
		if (clazz == null) {
			throw new IllegalArgumentException("参数错误, clazz=" + clazz);
		}
	}

	/**
	 * 校验sharding entity
	 * 
	 * @param entity
	 *            DBEntity
	 * 
	 * @throws IllegalArgumentException
	 *             校验失败
	 */
	public static void checkShardingEntity(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("参数错误, entity=" + entity);
		}

		Class<?> clazz = entity.getClass();
		Table table = clazz.getAnnotation(Table.class);
		if (table == null) {
			throw new IllegalArgumentException("参数错误, 实体对象需要使用@Table注解, class=" + clazz);
		}

		String clusterName = table.cluster();
		String shardingField = table.shardingBy();
		int shardingNum = table.shardingNum();

		if (StringUtils.isBlank(clusterName) || StringUtils.isBlank(shardingField) || shardingNum <= 0) {
			throw new IllegalArgumentException("被保存的对象不是ShardingEntity, class=" + clazz);
		}
	}

	/**
	 * 校验global entity
	 * 
	 * @param entity
	 */
	public static void checkGlobalEntity(Object entity) {
		if (entity == null) {
			throw new IllegalArgumentException("参数错误, entity=" + entity);
		}
		Class<?> clazz = entity.getClass();
		Table table = clazz.getAnnotation(Table.class);
		if (table == null) {
			throw new IllegalArgumentException("参数错误, 实体对象需要使用@Table注解, class=" + clazz);
		}
		String clusterName = table.cluster();
		String shardingField = table.shardingBy();
		int shardingNum = table.shardingNum();
		if (StringUtils.isBlank(clusterName) || StringUtils.isNotBlank(shardingField) || shardingNum > 0) {
			throw new IllegalArgumentException("被保存的对象不是GlobalEntity class=" + clazz);
		}
	}

}
