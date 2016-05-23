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

public class EnumArrayCodec implements Codec<Enum[]> {

    public void encode(DataOutput output, Enum[] v, CodecConfig config) throws CodecException {
        // write type
        output.writeByte(CodecType.TYPE_ARRAY_ENUM);

        // write is null
        if (v == null) {
            output.writeByte(CodecType.NULL);
            return;
        }
        output.writeByte(CodecType.NOT_NULL);

        // write length
        int length = v.length;
        output.writeVInt(length);

        boolean isWriteClass = false;
        // write value
        for (int i = 0; i < length; i++) {
            if (v[i] == null) {
                output.writeByte(CodecType.NULL);
                continue;
            }
            output.writeByte(CodecType.NOT_NULL);
            if (!isWriteClass) {
                output.writeGBK(v[i].getDeclaringClass().getName());
                isWriteClass = true;
            }
            output.writeGBK(v[i].name());
        }
    }

    public Enum[] decode(DataInput input, CodecConfig config) throws CodecException {
        // read is null
        if (input.readByte() == CodecType.NULL) {
            return null;
        }

        // read length
        int length = input.readVInt();

        boolean isReadClass = false;
        Object array = null;
        Class<Enum> oc = null;
        for (int i = 0; i < length; i++) {
            if (input.readByte() == CodecType.NOT_NULL) {
                if (!isReadClass) {
                    oc = (Class<Enum>) BeansUtil.getClass(input.readGBK());
                    array = Array.newInstance(oc, length);
                    isReadClass = true;
                }
                String name = input.readGBK();
                Array.set(array, i, Enum.valueOf(oc, name));
            }
        }
        return (Enum[]) array;
    }

}
