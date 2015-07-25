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

package org.pinus4j.utils;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.pinus4j.constant.Const;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.annotations.DateTime;
import org.pinus4j.entity.annotations.PrimaryKey;
import org.pinus4j.entity.annotations.UpdateTime;
import org.pinus4j.entity.meta.DBTable;
import org.pinus4j.entity.meta.DBTablePK;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.exceptions.DBOperationException;

import com.google.common.collect.Lists;

/**
 * 反射工具类. 提供了一些简单的反射功能. 方便其他操作调用.
 * 
 * @author duanbn
 */
public class ReflectUtil {

    /**
     * 字段别名缓存，当没有指定别名时，使用字段名作为别名. key: 别名/字段名
     */
    public static final Map<String, Field>     _aliasFieldCache  = new ConcurrentHashMap<String, Field>();

    /**
     * 类属性缓存. 缓存反射结果
     */
    public static final Map<Class<?>, Field[]> _fieldCache       = new ConcurrentHashMap<Class<?>, Field[]>();

    /**
     * 实体元信息管理
     */
    private static IEntityMetaManager          entityMetaManager = DefaultEntityMetaManager.getInstance();

    public static PKValue getNotUnionPkValue(Object obj) {
        PKName pkName = getNotUnionPkName(obj.getClass());

        Object pkValue = getProperty(obj, pkName.getValue());

        return PKValue.valueOf(pkValue);
    }

    /**
     * 获取主键值.
     *
     * @param obj
     * @return 主键值
     * @throws Exception 获取失败
     */
    public static EntityPK getPkValue(Object obj) {
        List<PKName> pkNames = getPkName(obj.getClass());
        List<PKValue> pkValues = Lists.newArrayList();
        Object pkValue = null;
        for (PKName pkName : pkNames) {
            pkValue = getProperty(obj, pkName.getValue());
            pkValues.add(PKValue.valueOf(pkValue));
        }
        return EntityPK.valueOf(pkNames.toArray(new PKName[pkNames.size()]),
                pkValues.toArray(new PKValue[pkValues.size()]));
    }

    public static PKName getNotUnionPkName(Class<?> clazz) {
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);
        if (dbTable.isUnionPrimaryKey()) {
            throw new IllegalStateException("不支持联合主键, class=" + clazz);
        }

        List<DBTablePK> primaryKeys = dbTable.getPrimaryKeys();

        if (primaryKeys.isEmpty()) {
            throw new IllegalStateException("找不到主键 class=" + clazz);
        }

        return primaryKeys.get(0).getPKName();
    }

    /**
     * 获取对象的主键字段名.
     *
     * @param clazz 获取此对象的数据库主键名
     * @return 字包含pkName的EntityPK对象
     */
    public static List<PKName> getPkName(Class<?> clazz) {
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        List<DBTablePK> primaryKeys = dbTable.getPrimaryKeys();

        if (primaryKeys.isEmpty()) {
            throw new IllegalStateException("找不到主键 class=" + clazz);
        }

        List<PKName> ePKList = new ArrayList<PKName>(primaryKeys.size());
        for (DBTablePK primaryKey : primaryKeys) {
            ePKList.add(primaryKey.getPKName());
        }

        return ePKList;
    }

    /**
     * 判断是否是分片数据对象.
     */
    public static boolean isShardingEntity(Class<?> clazz) {
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        return dbTable.isSharding();
    }

    /**
     * 获取sharding值
     * 
     * @param entity
     * @return
     */
    public static Object getShardingValue(Object entity) {
        Class<?> clazz = entity.getClass();
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        String shardingField = dbTable.getShardingBy();
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
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        return dbTable.getCluster();
    }

    /**
     * 获取集群表数量.
     * 
     * @param clazz
     * @return
     */
    public static int getTableNum(Class<?> clazz) {
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        return dbTable.getShardingNum();
    }

    /**
     * 获取表名.
     * 
     * @param entity 数据对象
     * @param tableIndex 表下标
     * @return 表名
     */
    public static String getTableName(Object entity, int tableIndex) {
        Class<?> entityClass = entity.getClass();
        return getTableName(entityClass, tableIndex);
    }

    /**
     * 获取表名. 如果下标等于-1则忽略添加下标
     * 
     * @param clazz 数据对象class
     * @param tableIndex 表下标
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
     * 获取表名不带分表下标.
     * 
     * @param clazz 数据对象class
     * @return 表名，不带分表下标
     */
    public static String getTableName(Class<?> clazz) {
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        return dbTable.getName();
    }

    /**
     * 判断实体是否需要被缓存
     * 
     * @param clazz 实体对象
     * @return true:是, false:否
     */
    public static boolean isCache(Class<?> clazz) {
        DBTable dbTable = entityMetaManager.getTableMeta(clazz);

        return dbTable.isCache();
    }

    /**
     * 通过反射获取对象的属性值.
     * 
     * @param obj 被反射对象
     * @param propertyName 属性名
     * @return 属性值
     * @throws Exception 操作失败
     */
    public static Object getProperty(Object obj, String propertyName) {
        Field f = getField(obj.getClass(), propertyName);
        f.setAccessible(true);
        try {
            return f.get(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过反射给对象属性赋值.
     * 
     * @param obj 被反射的对象
     * @param propertyName 赋值的属性名
     * @param value 值
     * @throws Exception 操作失败
     */
    public static void setProperty(Object obj, String propertyName, Object value) {
        Field f = getField(obj.getClass(), propertyName);
        f.setAccessible(true);

        try {
            if (value == null) {
                f.set(obj, null);
                return;
            }

            // 这里不能支持装箱类型，否则反射会报错
            if (f.getType() == Boolean.TYPE) {
                f.setBoolean(obj, ((Boolean) value).booleanValue());
            } else if (f.getType() == Integer.TYPE) {
                f.setInt(obj, ((Number) value).intValue());
            } else if (f.getType() == Byte.TYPE) {
                f.setByte(obj, ((Number) value).byteValue());
            } else if (f.getType() == Long.TYPE) {
                f.setLong(obj, ((Number) value).longValue());
            } else if (f.getType() == Short.TYPE) {
                f.setShort(obj, ((Number) value).shortValue());
            } else if (f.getType() == Float.TYPE) {
                f.setFloat(obj, ((Number) value).floatValue());
            } else if (f.getType() == Double.TYPE) {
                f.setDouble(obj, ((Number) value).doubleValue());
            } else if (f.getType() == Character.TYPE) {
                f.setChar(obj, ((Character) value).charValue());
            } else {
                f.set(obj, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取对象的属性名及属性值.
     * 
     * @param obj 被反射对象.
     * @return 属性名和属性值
     */
    public static Map<String, Object> describe(Object obj) throws Exception {
        return describe(obj, false);
    }

    /**
     * 获取对象的属性描述.
     * 
     * @param obj 被反射的对象
     * @param isFilteDefault 是否过滤掉默认值
     * @return {属性名, 属性值}
     */
    public static Map<String, Object> describe(Object obj, boolean isFilteNull) throws Exception {
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
            Class<?> fTypeClazz = f.getType();

            org.pinus4j.entity.annotations.Field annoField = f
                    .getAnnotation(org.pinus4j.entity.annotations.Field.class);
            if (fTypeClazz == String.class && annoField != null && annoField.length() > Const.COLUMN_TEXT_LENGTH
                    && value == null) {
                value = "";
            }

            // 过滤默认值
            if (isFilteNull) {
                if (value == null) {
                    continue;
                }

                if (fTypeClazz == Boolean.TYPE || fTypeClazz == Boolean.class) {
                    if (!(Boolean) value) {
                        continue;
                    }
                } else if (fTypeClazz == Byte.TYPE || fTypeClazz == Byte.class) {
                    if ((Byte) value == 0) {
                        continue;
                    }
                } else if (fTypeClazz == Character.TYPE || fTypeClazz == Character.class) {
                    if ((Character) value == 0) {
                        continue;
                    }
                } else if (fTypeClazz == Short.TYPE || fTypeClazz == Short.class) {
                    if ((Short) value == 0) {
                        continue;
                    }
                } else if (fTypeClazz == Integer.TYPE || fTypeClazz == Integer.class) {
                    if ((Integer) value == 0) {
                        continue;
                    }
                } else if (fTypeClazz == Long.TYPE || fTypeClazz == Long.class) {
                    if ((Long) value == 0l) {
                        continue;
                    }
                } else if (fTypeClazz == Float.TYPE || fTypeClazz == Float.class) {
                    if ((Float) value == 0.0f) {
                        continue;
                    }
                } else if (fTypeClazz == Double.TYPE || fTypeClazz == Double.class) {
                    if ((Double) value == 0.0) {
                        continue;
                    }
                }

            }

            map.put(getFieldName(f), value);
        }

        return map;
    }

    public static void putAliasField(Class<?> clazz, String fieldName, Field f) {
        _aliasFieldCache.put(clazz.getName() + fieldName, f);
    }

    /**
     * @param clazz
     * @param field
     * @return
     */
    public static Field getField(Class<?> clazz, String fieldName) {
        Field field = _aliasFieldCache.get(clazz.getName() + fieldName);
        if (field == null) {
            try {
                field = clazz.getDeclaredField(fieldName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return field;
    }

    /**
     * 获取字段名，包括别名
     * 
     * @param field
     * @return
     */
    public static String getFieldName(Field field) {
        // 兼容字段别名
        String fieldName = field.getName();
        org.pinus4j.entity.annotations.Field annoField = field
                .getAnnotation(org.pinus4j.entity.annotations.Field.class);
        if (annoField != null && StringUtils.isNotBlank(annoField.name())) {
            fieldName = annoField.name();
        }

        PrimaryKey annoPrimaryKey = field.getAnnotation(PrimaryKey.class);
        if (annoPrimaryKey != null && StringUtils.isNotBlank(annoPrimaryKey.name())) {
            fieldName = annoPrimaryKey.name();
        }

        DateTime annoDateTime = field.getAnnotation(DateTime.class);
        if (annoDateTime != null && StringUtils.isNotBlank(annoDateTime.name())) {
            fieldName = annoDateTime.name();
        }

        UpdateTime annoUpdateTime = field.getAnnotation(UpdateTime.class);
        if (annoUpdateTime != null && StringUtils.isNotBlank(annoUpdateTime.name())) {
            fieldName = annoUpdateTime.name();
        }

        return fieldName;
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
            } else if (f.getAnnotation(org.pinus4j.entity.annotations.Field.class) != null) {
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

    /**
     * 克隆一个对象，只保留给定的属性值.
     *
     * @param obj 被克隆的对象.
     * @param fieldNames 需要被保留的属性名.
     * @return 克隆对象.
     */
    public static Object cloneWithGivenField(Object obj, String... fieldNames) throws Exception {
        if (fieldNames == null || fieldNames.length == 0) {
            return obj;
        }

        Object clone = obj.getClass().newInstance();
        Object value = null;
        for (String fieldName : fieldNames) {
            value = getProperty(obj, fieldName);
            setProperty(clone, fieldName, value);
        }
        return clone;
    }

}
