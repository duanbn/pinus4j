package com.pinus.util;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.pinus.api.annotation.DateTime;
import com.pinus.api.annotation.PrimaryKey;
import com.pinus.api.annotation.Table;
import com.pinus.api.annotation.UpdateTime;
import com.pinus.exception.DBOperationException;

/**
 * 反射工具类. 提供了一些简单的反射功能. 方便其他操作调用.
 * 
 * @author duanbn
 */
public class ReflectUtil {

	/**
	 * 主键字段名缓存.
	 */
	private static final Map<Class<?>, String> _pkNameCache = new ConcurrentHashMap<Class<?>, String>();

	/**
	 * 缓存被需要被缓存的表
	 */
	public static final Map<Class<?>, Boolean> _tableCachedCache = new ConcurrentHashMap<Class<?>, Boolean>();

	/**
	 * 类属性缓存. 缓存反射结果
	 */
	public static final Map<Class<?>, Field[]> _fieldCache = new ConcurrentHashMap<Class<?>, Field[]>();

	/**
	 * 集群名缓存.
	 */
	public static final Map<Class<?>, String> _clusterNameCache = new ConcurrentHashMap<Class<?>, String>();

	/**
	 * 数据表名缓存.
	 */
	private static final Map<Class<?>, String> _tableNameCache = new ConcurrentHashMap<Class<?>, String>();

	/**
	 * 数据分片字段缓存.
	 */
	private static final Map<Class<?>, String> _shardingFieldCache = new ConcurrentHashMap<Class<?>, String>();

	/**
	 * 集群表数量
	 */
	private static final Map<Class<?>, Integer> _tableNumCache = new ConcurrentHashMap<Class<?>, Integer>();

	/**
	 * 获取主键值.
	 * 
	 * @param obj
	 * @return 主键值
	 * @throws Exception
	 *             获取失败
	 */
	public static Number getPkValue(Object obj) {
		String pkName = getPkName(obj.getClass());
		try {
			return (Number) getProperty(obj, pkName);
		} catch (Exception e) {
			throw new RuntimeException("获取主键值失败" + e);
		}
	}

	/**
	 * 设置主键
	 * 
	 * @param obj
	 * @throws Exception
	 */
	public static void setPkValue(Object obj, Number pk) throws Exception {
		try {
			String pkName = getPkName(obj.getClass());
			setProperty(obj, pkName, pk);
		} catch (Exception e) {
			throw new Exception("设置主键值失败", e);
		}
	}

	/**
	 * 获取对象的主键字段名.
	 * 
	 * @param clazz
	 *            获取此对象的数据库主键名
	 * 
	 * @return 主键名
	 */
	public static String getPkName(Class<?> clazz) {
		String pkName = _pkNameCache.get(clazz);
		if (pkName != null) {
			return pkName;
		}

		// 便利属性找到@PrimaryKey标识的属性名
		Field[] fields = ReflectUtil.getFields(clazz);
		for (Field f : fields) {
			if (f.getAnnotation(PrimaryKey.class) != null) {
				pkName = f.getName();
				break;
			}
		}

		if (pkName == null) {
			throw new IllegalArgumentException("没有标注主键属性, class=" + clazz);
		}

		_pkNameCache.put(clazz, pkName);

		return pkName;
	}

	/**
	 * 获取sharding值
	 * 
	 * @param entity
	 * @return
	 */
	public static Object getShardingVAlue(Object entity) {
		Class<?> clazz = entity.getClass();
		String shardingField = _shardingFieldCache.get(clazz);
		if (shardingField == null) {
			Table annoTable = clazz.getAnnotation(Table.class);
			if (annoTable == null) {
				throw new IllegalArgumentException(clazz + "无法分片的数据实体，请使用@Table注解");
			}
			shardingField = annoTable.shardingBy();

			_shardingFieldCache.put(clazz, shardingField);
		}

		Object shardingValue = null;
		try {
			shardingValue = getProperty(entity, shardingField);
		} catch (Exception e) {
			throw new DBOperationException("获取sharding value失败, clazz=" + clazz + " field=" + shardingField);
		}
		if (shardingValue == null) {
			throw new IllegalStateException("shardingValue is null, clazz=" + clazz + " field=" + shardingField);
		}

		return shardingValue;
	}

	/**
	 * 获取集群名
	 * 
	 * @param clazz
	 * @return
	 */
	public static String getClusterName(Class<?> clazz) {
		String clusterName = _clusterNameCache.get(clazz);
		if (clusterName == null) {
			Table annoTable = clazz.getAnnotation(Table.class);
			if (annoTable == null) {
				throw new IllegalArgumentException(clazz + "无法分片的数据实体，请使用@Table注解");
			}
			clusterName = annoTable.cluster();

			_clusterNameCache.put(clazz, clusterName);
		}

		return clusterName;
	}

	/**
	 * 获取集群表数量.
	 * 
	 * @param clazz
	 * @return
	 */
	public static int getTableNum(Class<?> clazz) {
		Integer tableNum = _tableNumCache.get(clazz);
		if (tableNum == null) {
			Table annoTable = clazz.getAnnotation(Table.class);
			if (annoTable == null) {
				throw new IllegalArgumentException(clazz + "无法分片的数据实体，请使用@Table注解");
			}
			tableNum = annoTable.shardingNum();

			_tableNumCache.put(clazz, tableNum);
		}

		return tableNum;
	}

	/**
	 * 获取表名.
	 * 
	 * @param entity
	 *            数据对象
	 * @param tableIndex
	 *            表下标
	 * 
	 * @return 表名
	 */
	public static String getTableName(Object entity, int tableIndex) {
		Class<?> entityClass = entity.getClass();
		return getTableName(entityClass, tableIndex);
	}

	/**
	 * 获取表名. 如果下标等于-1则忽略添加下标
	 * 
	 * @param clazz
	 *            数据对象class
	 * @param tableIndex
	 *            表下标
	 * 
	 * @return 表名
	 */
	public static String getTableName(Class<?> clazz, int tableIndex) {
		if (tableIndex == -1) {
			return getTableName(clazz);
		} else {
			return getTableName(clazz) + tableIndex;
		}
	}

	/**
	 * 判断实体是否需要被缓存
	 * 
	 * @param clazz
	 *            实体对象
	 * @return true:是, false:否
	 */
	public static boolean isCache(Class<?> clazz) {
		Boolean isCache = _tableCachedCache.get(clazz);
		if (isCache != null) {
			return isCache;
		}

		isCache = clazz.getAnnotation(Table.class).cache();
		_tableCachedCache.put(clazz, isCache);

		return isCache;
	}

	/**
	 * 获取表名不带分表下标.
	 * 
	 * @param clazz
	 *            数据对象class
	 * 
	 * @return 表名，不带分表下标
	 */
	public static String getTableName(Class<?> clazz) {
		String tableName = _tableNameCache.get(clazz);
		if (tableName != null) {
			return tableName;
		}

		Table annoTable = clazz.getAnnotation(Table.class);
		if (annoTable == null) {
			throw new IllegalArgumentException(clazz + "无法分片的数据实体，请使用@Table注解");
		}
		tableName = StringUtils.isBlank(annoTable.name()) ? clazz.getSimpleName() : annoTable.name();

		tableName = tableName.toLowerCase();
		_tableNameCache.put(clazz, tableName);

		return tableName;
	}

	/**
	 * 通过反射获取对象的属性值.
	 * 
	 * @param obj
	 *            被反射对象
	 * @param propertyName
	 *            属性名
	 * @return 属性值
	 * @throws Exception
	 *             操作失败
	 */
	public static Object getProperty(Object obj, String propertyName) throws Exception {
		Field f = obj.getClass().getDeclaredField(propertyName);
		f.setAccessible(true);
		return f.get(obj);
	}

	/**
	 * 通过反射给对象属性赋值.
	 * 
	 * @param obj
	 *            被反射的对象
	 * @param propertyName
	 *            赋值的属性名
	 * @param value
	 *            值
	 * @throws Exception
	 *             操作失败
	 */
	public static void setProperty(Object obj, String propertyName, Object value) throws Exception {
		Field f = obj.getClass().getDeclaredField(propertyName);
		f.setAccessible(true);
		if (value instanceof Number) {
			Number numValue = (Number) value;
			if (f.getType() == Integer.TYPE || f.getType() == Integer.class) {
				f.setInt(obj, numValue.intValue());
			} else if (f.getType() == Long.TYPE || f.getType() == Long.class) {
				f.setLong(obj, numValue.longValue());
			} else if (f.getType() == Short.TYPE || f.getType() == Short.class) {
				f.setShort(obj, numValue.shortValue());
			} else if (f.getType() == Float.TYPE || f.getType() == Float.class) {
				f.setFloat(obj, numValue.floatValue());
			} else if (f.getType() == Double.TYPE || f.getType() == Double.class) {
				f.setDouble(obj, numValue.doubleValue());
			} else {
				throw new IllegalArgumentException("无法识别的值类型");
			}
		} else {
			f.set(obj, value);
		}
	}

	/**
	 * 获取对象的描述并过滤某些注解的属性.
	 * 
	 * @param obj
	 *            被反射的对象
	 * @param filteAnnos
	 *            被过滤的注解
	 * 
	 * @return {属性名, 属性值}
	 */
	public static Map<String, Object> describeWithoutUpdateTime(Object obj, boolean filteNull) throws Exception {
		if (obj == null) {
			throw new IllegalArgumentException("参数错误, obj=null");
		}

		Class<?> objClass = obj.getClass();
		Map<String, Object> map = new TreeMap<String, Object>();
		Object value = null;
		for (Field f : getFields(objClass)) {
			f.setAccessible(true);

			if (f.getAnnotation(UpdateTime.class) != null) {
				f.set(obj, new Timestamp(System.currentTimeMillis()));
				continue;
			}

			value = f.get(obj);

			if (filteNull && value == null) {
				continue;
			}

			map.put(f.getName(), f.get(obj));
		}

		return map;
	}

	/**
	 * 获取对象的属性描述.
	 * 
	 * @param obj
	 *            被反射的对象
	 * @param filteNull
	 *            是否过滤掉null值
	 * @param filteAnnos
	 *            被过滤的注解
	 * 
	 * @return {属性名, 属性值}
	 */
	public static Map<String, Object> describe(Object obj, boolean filteNull) throws Exception {
		if (obj == null) {
			throw new IllegalArgumentException("参数错误, obj=null");
		}

		Class<?> objClass = obj.getClass();
		Map<String, Object> map = new TreeMap<String, Object>();
		Object value = null;
		for (Field f : getFields(objClass)) {
			f.setAccessible(true);

			if (f.getAnnotation(UpdateTime.class) != null) {
				f.set(obj, new Timestamp(System.currentTimeMillis()));
			}

			value = f.get(obj);

			if (filteNull && value == null) {
				continue;
			}

			map.put(f.getName(), f.get(obj));
		}

		return map;
	}

	/**
	 * 获取对象的属性名及属性值.
	 * 
	 * @param obj
	 *            被反射对象.
	 * @return 属性名和属性值
	 */
	public static Map<String, Object> describe(Object obj) throws Exception {
		return describe(obj, false);
	}

	/**
	 * 获取类的所有属性名.
	 * 
	 * @return 字段名
	 */
	public static Field[] getFields(Class<?> clazz) {
		Field[] fields = _fieldCache.get(clazz);
		if (fields != null) {
			return fields;
		}

		List<Field> mappingFields = new ArrayList<Field>();
		for (Field f : clazz.getDeclaredFields()) {
			if (f.getAnnotation(PrimaryKey.class) != null) {
				mappingFields.add(f);
			} else if (f.getAnnotation(com.pinus.api.annotation.Field.class) != null) {
				mappingFields.add(f);
			} else if (f.getAnnotation(DateTime.class) != null) {
				mappingFields.add(f);
			} else if (f.getAnnotation(UpdateTime.class) != null) {
				mappingFields.add(f);
			}
		}
		if (mappingFields.isEmpty()) {
			throw new IllegalStateException("没有包含可以操作的列属性" + clazz);
		}

		fields = mappingFields.toArray(new Field[mappingFields.size()]);
		_fieldCache.put(clazz, fields);

		return fields;
	}

}
