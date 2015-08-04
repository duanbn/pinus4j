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

import java.util.Set;

import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;

/**
 * 列表编码解码.
 *
 * @author duanbn
 */
public class SetCodec implements Codec<Set<Object>>
{

    public void encode(DataOutput output, Set<Object> v, CodecConfig config) throws CodecException
    {
        output.writeByte(CodecType.TYPE_SET);

        // write set instance type
        byte type = -1;
        type = config.getCodecType(v);
        if (type == -1) {
            throw new CodecException("不可被序列化为Set的类型(" + v.getClass() + ")");
        }
        output.writeByte(type);

        try {
            Set<?> set = (Set<?>) v;
            int length = set.size();
            output.writeVInt(length); // write length

            for (Object obj : set) {
                if (obj == null) {
                    output.writeByte(CodecType.NULL);
                } else {
                    output.writeByte(CodecType.NOT_NULL);
                    config.lookup(obj).encode(output, obj, config);
                }
            }
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

    public Set<Object> decode(DataInput input, CodecConfig config) throws CodecException
    {
        try {
            byte type = input.readByte();

            Set set = (Set) config.getClassByType(type).newInstance();

            int length = input.readVInt(); // read length

            for (int i=0; i<length; i++) {
                if (input.readByte() != CodecType.NULL) {
                    type = input.readByte();
                    set.add(config.lookup(type).decode(input, config));
                }
            }
            
            return set;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
