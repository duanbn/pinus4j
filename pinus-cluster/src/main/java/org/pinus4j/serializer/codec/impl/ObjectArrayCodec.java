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

import java.lang.reflect.Array;

import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;
import org.pinus4j.utils.BeansUtil;

/**
 * 对对象数组进行编码.
 *
 * @author duanbn
 */
public class ObjectArrayCodec implements Codec<Object[]> {

    public void encode(DataOutput output, Object[] v, CodecConfig config) throws CodecException {
        try {
            // write type
            output.writeByte(CodecType.TYPE_ARRAY_OBJECT);

            // write is null
            if (v == null) {
                output.writeByte(CodecType.NULL);
                return;
            }
            output.writeByte(CodecType.NOT_NULL);

            // write array class name
            output.writeGBK(v.getClass().getComponentType().getName());
            // write length
            int length = Array.getLength(v);
            output.writeVInt(length);

            // write array value
            Object arrayValue = null;
            Codec codec = null;
            for (int i = 0; i < length; i++) {
                arrayValue = Array.get(v, i);
                if (arrayValue == null) {
                    output.writeByte(CodecType.NULL);
                    continue;
                }
                output.writeByte(CodecType.NOT_NULL);
                codec = config.lookup(arrayValue);
                codec.encode(output, arrayValue, config);
            }
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

    public Object[] decode(DataInput input, CodecConfig config) throws CodecException {
        try {
            // read is null
            if (input.readByte() == CodecType.NULL) {
                return null;
            }

            // read array class name
            Class<?> arrayClass = BeansUtil.getClass(input.readGBK());
            // read length
            int length = input.readVInt();

            // read array value
            Object array = Array.newInstance(arrayClass, length);
            Codec codec = null;
            for (int i = 0; i < length; i++) {
                if (input.readByte() == CodecType.NOT_NULL) {
                    byte type = input.readByte();
                    codec = config.lookup(type);
                    Object arrayValue = codec.decode(input, config);
                    Array.set(array, i, arrayValue);
                }
            }
            return (Object[]) array;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
