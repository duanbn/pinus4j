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

import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;

/**
 * @author duanbn
 */
public class ClassCodec implements Codec<Class>
{

    public void encode(DataOutput output, Class v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_CLASS);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeUTF8(v.getName());
        }
    }

    public Class decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            String className = input.readUTF8();
            if (Boolean.TYPE.getName().equals(className)) {
                return Boolean.TYPE;
            } else if (Byte.TYPE.getName().equals(className)) {
                return Byte.TYPE;
            } else if (Character.TYPE.getName().equals(className)) {
                return Character.TYPE;
            } else if (Short.TYPE.getName().equals(className)) {
                return Short.TYPE;
            } else if (Integer.TYPE.getName().equals(className)) {
                return Integer.TYPE;
            } else if (Long.TYPE.getName().equals(className)) {
                return Long.TYPE;
            } else if (Float.TYPE.getName().equals(className)) {
                return Float.TYPE;
            } else if (Double.TYPE.getName().equals(className)) {
                return Double.TYPE;
            }

            Class c = loader.loadClass(className);
            return c;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
