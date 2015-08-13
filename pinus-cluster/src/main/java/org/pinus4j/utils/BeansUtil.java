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
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.pinus4j.constant.Const;
import org.pinus4j.entity.annotations.DateTime;
import org.pinus4j.entity.annotations.PrimaryKey;
import org.pinus4j.entity.annotations.UpdateTime;

/**
 * 反射工具类. 提供了一些简单的反射功能. 方便其他操作调用.
 * 
 * @author duanbn
 */
public class BeansUtil {

    /**
     * 字段别名缓存，当没有指定别名时，使用字段名作为别名. key: 别名/字段名
     */
    public static final Map<String, Field>       _aliasFieldCache = new ConcurrentHashMap<String, Field>();

    /**
     * 类属性缓存. 缓存反射结果
     */
    public static final Map<Class<?>, Field[]>   _fieldCache      = new ConcurrentHashMap<Class<?>, Field[]>();

    /**
     * 接口的缓存
     */
    private static final Map<String, Class<?>[]> interfaceCache   = new HashMap<String, Class<?>[]>();

    public static final Map<String, Class<?>>    classCache       = new HashMap<String, Class<?>>();

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
        if (obj == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        Class<?> clazz = obj.getClass();

        Field f = getField(clazz, propertyName);

        if (f != null) {
            try {
                f.setAccessible(true);

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
        } else {
            Method[] setMethods = clazz.getMethods();
            for (Method setMethod : setMethods) {
                if (setMethod.getName().equals("set" + StringUtil.upperFirstLetter(propertyName))) {
                    if (setMethod.getParameterTypes().length == 1) {
                        try {
                            setMethod.invoke(obj, value);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return;
                    }
                }
            }
        }

    }

    @Deprecated
    public static void setProperty(Object obj, String propertyName, String value) {
        if (obj == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        Class<?> clazz = obj.getClass();

        Field f = getField(clazz, propertyName);
        if (f != null) {
            try {
                f.setAccessible(true);
                if (value == null) {
                    f.set(obj, null);
                    return;
                }

                // 这里不能支持装箱类型，否则反射会报错
                if (f.getType() == Boolean.TYPE || f.getType() == Boolean.class) {
                    f.setBoolean(obj, (Boolean.valueOf(value)).booleanValue());
                } else if (f.getType() == Integer.TYPE || f.getType() == Integer.class) {
                    f.setInt(obj, Integer.parseInt(value));
                } else if (f.getType() == Byte.TYPE || f.getType() == Byte.class) {
                    f.setByte(obj, Byte.parseByte(value));
                } else if (f.getType() == Long.TYPE || f.getType() == Long.class) {
                    f.setLong(obj, Long.parseLong(value));
                } else if (f.getType() == Short.TYPE || f.getType() == Short.class) {
                    f.setShort(obj, Short.valueOf(value));
                } else if (f.getType() == Float.TYPE || f.getType() == Float.class) {
                    f.setFloat(obj, Float.valueOf(value));
                } else if (f.getType() == Double.TYPE || f.getType() == Double.class) {
                    f.setDouble(obj, Double.valueOf(value));
                } else if (f.getType() == Character.TYPE || f.getType() == Character.class) {
                    f.setChar(obj, value.charAt(0));
                } else {
                    f.set(obj, value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            Method[] setMethods = clazz.getMethods();
            for (Method setMethod : setMethods) {
                if (setMethod.getName().equals("set" + StringUtil.upperFirstLetter(propertyName))) {
                    if (setMethod.getParameterTypes().length == 1) {
                        Class<?> paramType = setMethod.getParameterTypes()[0];
                        try {
                            if (paramType == Boolean.TYPE || paramType == Boolean.class) {
                                setMethod.invoke(obj, (Boolean.valueOf(value)).booleanValue());
                            } else if (paramType == Integer.TYPE || paramType == Integer.class) {
                                setMethod.invoke(obj, Integer.parseInt(value));
                            } else if (paramType == Byte.TYPE || paramType == Byte.class) {
                                setMethod.invoke(obj, Byte.parseByte(value));
                            } else if (paramType == Long.TYPE || paramType == Long.class) {
                                setMethod.invoke(obj, Long.parseLong(value));
                            } else if (paramType == Short.TYPE || paramType == Short.class) {
                                setMethod.invoke(obj, Short.valueOf(value));
                            } else if (paramType == Float.TYPE || paramType == Float.class) {
                                setMethod.invoke(obj, Float.valueOf(value));
                            } else if (paramType == Double.TYPE || paramType == Double.class) {
                                setMethod.invoke(obj, Double.valueOf(value));
                            } else if (paramType == Character.TYPE || paramType == Character.class) {
                                setMethod.invoke(obj, value.charAt(0));
                            } else {
                                setMethod.invoke(obj, value);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return;
                    }
                }
            }
        }
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

                //                if (fTypeClazz == Boolean.TYPE || fTypeClazz == Boolean.class) {
                //                    if (!(Boolean) value) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Byte.TYPE || fTypeClazz == Byte.class) {
                //                    if ((Byte) value == 0) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Character.TYPE || fTypeClazz == Character.class) {
                //                    if ((Character) value == 0) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Short.TYPE || fTypeClazz == Short.class) {
                //                    if ((Short) value == 0) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Integer.TYPE || fTypeClazz == Integer.class) {
                //                    if ((Integer) value == 0) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Long.TYPE || fTypeClazz == Long.class) {
                //                    if ((Long) value == 0l) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Float.TYPE || fTypeClazz == Float.class) {
                //                    if ((Float) value == 0.0f) {
                //                        continue;
                //                    }
                //                } else if (fTypeClazz == Double.TYPE || fTypeClazz == Double.class) {
                //                    if ((Double) value == 0.0) {
                //                        continue;
                //                    }
                //                }

            }

            map.put(getFieldName(f), value);
        }

        return map;
    }

    public static void copyProperties(Object source, Object target) throws Exception {
        if (source == null) {
            throw new IllegalArgumentException("source should not be null");
        }
        if (target == null) {
            throw new IllegalArgumentException("target should not be null");
        }

        Field targetField = null;
        for (Field sourceField : getFields(source.getClass())) {
            sourceField.setAccessible(true);

            targetField = target.getClass().getDeclaredField(sourceField.getName());
            targetField.setAccessible(true);

            if (targetField != null) {
                targetField.set(target, sourceField.get(source));
            }
        }
    }

    public static void putAliasField(Class<?> clazz, String fieldName, Field f) {
        _aliasFieldCache.put(clazz.getName() + fieldName, f);
    }

    /**
     * 支持通过别名获取Field
     * 
     * @param clazz
     * @param field
     * @return
     */
    public static Field getField(Class<?> clazz, String fieldName) {

        // 优先根据别名来获取
        Field field = _aliasFieldCache.get(clazz.getName() + fieldName);

        // 根据别名获取不到时
        if (field == null) {
            try {
                field = clazz.getDeclaredField(fieldName);
            } catch (Exception e) {
                // 忽略错误
            }
        }

        // 查找父类
        if (field == null) {
            Class<?> superClass = clazz.getSuperclass();
            if (superClass != null) {
                field = getField(superClass, fieldName);
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
        if (annoField != null && StringUtil.isNotBlank(annoField.name())) {
            fieldName = annoField.name();
        }

        PrimaryKey annoPrimaryKey = field.getAnnotation(PrimaryKey.class);
        if (annoPrimaryKey != null && StringUtil.isNotBlank(annoPrimaryKey.name())) {
            fieldName = annoPrimaryKey.name();
        }

        DateTime annoDateTime = field.getAnnotation(DateTime.class);
        if (annoDateTime != null && StringUtil.isNotBlank(annoDateTime.name())) {
            fieldName = annoDateTime.name();
        }

        UpdateTime annoUpdateTime = field.getAnnotation(UpdateTime.class);
        if (annoUpdateTime != null && StringUtil.isNotBlank(annoUpdateTime.name())) {
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

    /**
     * 根据字符串获取Class
     *
     * @param name class全名
     * @return Class
     */
    public static Class<?> getClass(String name) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class<?> c = classCache.get(name);
            if (c == null) {
                c = loader.loadClass(name);
                classCache.put(name, c);
            }
            return c;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 获取一个对象实现的接口.
     *
     * @param clazz 对象的class
     * @return 接口的class
     */
    public static Class<?>[] getInterfaces(Class<?> clazz) {
        Class<?>[] interfaces = interfaceCache.get(clazz.getName());
        if (interfaces == null) {

            List<Class<?>> classList = new ArrayList<Class<?>>();

            Class<?> curClass = clazz;
            while (curClass != Object.class) {
                for (Class<?> interf : curClass.getInterfaces()) {
                    if (!classList.contains(interf)) {
                        classList.add(interf);
                    }
                }
                curClass = curClass.getSuperclass();
            }

            interfaces = classList.toArray(new Class<?>[classList.size()]);

            interfaceCache.put(clazz.getName(), interfaces);
        }

        return interfaces;
    }

}
