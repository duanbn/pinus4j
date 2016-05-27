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

package org.pinus4j.serializer.codec.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.ReflectUtil;

/**
 * 这个类只能序列化自定义对象，不能将基本类型当作对象来进行序列化，否则会发生错误. 基本类型的序列化使用相关的编码类.
 *
 * @author duanbn
 * @since 1.0
 */
public class ObjectCodec implements Codec<Object> {

    public void encode(DataOutput output, Object v, CodecConfig config) throws CodecException {
        try {
            output.writeByte(CodecType.TYPE_OBJECT); // write type
            if (v == null) {
                output.writeByte(CodecType.NULL); // write isnull
                return;
            }
            output.writeByte(CodecType.NOT_NULL); // write is not null

            Class<?> oc = v.getClass();
            output.writeGBK(oc.getName()); // write classname

            Object fvalue = null;
            Codec codec = null;

            for (Field f : ReflectUtil.getFields(oc)) { // write field
                if (!f.isAccessible()) {
                    f.setAccessible(true);
                }

                // 基本类型序列化
                if (f.getType() == Boolean.TYPE) {
                    output.writeByte(CodecType.TYPE_BOOLEAN);
                    output.writeBoolean(f.getBoolean(v));
                    continue;
                } else if (f.getType() == Byte.TYPE) {
                    output.writeByte(CodecType.TYPE_BYTE);
                    output.writeByte(f.getByte(v));
                    continue;
                } else if (f.getType() == Character.TYPE) {
                    output.writeByte(CodecType.TYPE_CHAR);
                    output.writeChar(f.getChar(v));
                    continue;
                } else if (f.getType() == Short.TYPE) {
                    output.writeByte(CodecType.TYPE_SHORT);
                    output.writeShort(f.getShort(v));
                    continue;
                } else if (f.getType() == Integer.TYPE) {
                    output.writeByte(CodecType.TYPE_INT);
                    output.writeInt(f.getInt(v));
                    continue;
                } else if (f.getType() == Long.TYPE) {
                    output.writeByte(CodecType.TYPE_LONG);
                    output.writeLong(f.getLong(v));
                    continue;
                } else if (f.getType() == Float.TYPE) {
                    output.writeByte(CodecType.TYPE_FLOAT);
                    output.writeFloat(f.getFloat(v));
                    continue;
                } else if (f.getType() == Double.TYPE) {
                    output.writeByte(CodecType.TYPE_DOUBLE);
                    output.writeDouble(f.getDouble(v));
                    continue;
                }

                fvalue = f.get(v);
                if (fvalue == null) { // write field isnull
                    output.writeByte(CodecType.NULL);
                    continue;
                }
                output.writeByte(CodecType.NOT_NULL);

                codec = config.lookup(fvalue);
                codec.encode(output, fvalue, config);
            }

        } catch (IllegalAccessException e) {
            throw new CodecException(e);
        }
    }

    public Object decode(DataInput input, CodecConfig config) throws CodecException {
        try {
            if (input.readByte() == CodecType.NULL) { // read isnull
                return null;
            }

            Class<?> oc = BeansUtil.getClass(input.readGBK()); // read classname

            Object instance = newObject(oc);

            Object fvalue = null;
            Codec<?> codec = null;
            for (Field f : ReflectUtil.getFields(oc)) { // read field
                if (!f.isAccessible()) {
                    f.setAccessible(true);
                }

                byte type = input.readByte();

                // 基本类型反序列化
                if (type == CodecType.TYPE_BOOLEAN) {
                    f.setBoolean(instance, input.readBoolean());
                    continue;
                } else if (type == CodecType.TYPE_BYTE) {
                    f.setByte(instance, input.readByte());
                    continue;
                } else if (type == CodecType.TYPE_CHAR) {
                    f.setChar(instance, input.readChar());
                    continue;
                } else if (type == CodecType.TYPE_INT) {
                    f.setInt(instance, input.readInt());
                    continue;
                } else if (type == CodecType.TYPE_SHORT) {
                    f.setShort(instance, input.readShort());
                    continue;
                } else if (type == CodecType.TYPE_LONG) {
                    f.setLong(instance, input.readLong());
                    continue;
                } else if (type == CodecType.TYPE_FLOAT) {
                    f.setFloat(instance, input.readFloat());
                    continue;
                } else if (type == CodecType.TYPE_DOUBLE) {
                    f.setDouble(instance, input.readDouble());
                    continue;
                }

                if (type == CodecType.NULL) {
                    f.set(instance, null);
                    continue;
                }
                type = input.readByte();

                codec = config.lookup(type);
                fvalue = codec.decode(input, config);

                f.set(instance, fvalue);
            }
            return instance;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

    // FIXME: 这里需要对内部类进行特殊处理.
    private Object newObject(Class<?> clazz) throws InstantiationException, IllegalAccessException {

        Object instance = null;
        try {
            instance = clazz.newInstance();
        } catch (InstantiationException e) {
            Constructor<?>[] constructors = clazz.getConstructors();
            for (Constructor<?> c : constructors) {
                Class<?>[] paramClasses = c.getParameterTypes();
                Object[] paramObj = new Object[paramClasses.length];
                for (int i = 0; i < paramClasses.length; i++) {
                    paramObj[i] = newObject(paramClasses[i]);
                }
                try {
                    instance = c.newInstance(paramObj);
                } catch (Exception e1) {
                }

                if (instance != null) {
                    break;
                }
            }
        }

        if (instance == null) {
            throw new InstantiationException();
        }

        return instance;
    }
}
