package com.pinus.core.util;

import java.lang.reflect.*;
import java.util.*;

import com.pinus.core.ser.*;

import org.apache.log4j.Logger;

public class ReflectUtil
{
    public static final Logger log = Logger.getLogger(ReflectUtil.class);

    private static final Map<Class<?>, Field[]> fieldCache = new HashMap<Class<?>, Field[]>();

    /**
     * 接口的缓存
     */
    private static final Map<String, Class<?>[]> interfaceCache = new HashMap<String, Class<?>[]>();

    /**
     * 根据字符床获取Class
     *
     * @param name class全名
     *
     * @return Class
     */
    public static Class<?> getClass(String name)
    {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class<?> c = loader.loadClass(name);
            return c;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 获取一个对象的所有定义字段.
     * 从父类开始算起，第一个被标志为壳序列化的类的字段及其子类的字段的可继承字段
     * 当前类的所有字段.
     *
     * @param clazz 对象的class.
     *
     * @return 定义字段.
     */
    public static Field[] getFields(Class<?> clazz)
    {
        Field[] fields = fieldCache.get(clazz);
        //Field[] fields = null;

        if (fields == null) {
            // 获取所有的父类，判断从哪个父类开始有序列化能力.
            List<Class<?>> c1 = new ArrayList<Class<?>>();
            Class<?> curClass = clazz;
            while (curClass != Object.class) {
                c1.add(curClass);
                curClass = curClass.getSuperclass();
            }
            int index = 0;
            for (int i=c1.size()-1; i>=0; i--) {
                Class<?> checkClass = c1.get(i);
                for (Class<?> interf : checkClass.getInterfaces()) {
                    if (interf == Serializable.class) {
                        index = i;
                        break;
                    }
                }
            }
            // 存放有序列化能力的对象
            List<Class<?>> c2 = new ArrayList<Class<?>>(); 
            c2.addAll(c1.subList(0, index+1));

            // 获取当前类的私有属性, 不要父类的私有属性
            List<Field> fieldList = new ArrayList<Field>();
            boolean curPrivate = true;
            for (Class<?> c3 : c2) {
                for (Field f : c3.getDeclaredFields()) {
                    if (Modifier.isFinal(f.getModifiers())) {
                        continue;
                    }
                    if (Modifier.isPrivate(f.getModifiers()) && !curPrivate) {
                        continue;
                    }
                    if (!fieldList.contains(f)) {
                        fieldList.add(f);
                    }
                }
                curPrivate = false;
            }

            // 放入缓存
            fields = fieldList.toArray(new Field[fieldList.size()]);
            //return fields;
            fieldCache.put(clazz, fields);
        }

        return fieldCache.get(clazz);
    }

    /**
     * 获取一个对象实现的接口.
     *
     * @param clazz 对象的class
     *
     * @return 接口的class
     */
    public static Class<?>[] getInterfaces(Class<?> clazz)
    {
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

        return interfaceCache.get(clazz.getName());
    }

}
