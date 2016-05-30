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

import java.util.Map;

import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;

/**
 * @author duanbn
 */
public class MapCodec implements Codec<Map<Object, Object>> {

    public void encode(DataOutput output, Map<Object, Object> v, CodecConfig config) throws CodecException {
        output.writeByte(CodecType.TYPE_MAP);

        // write map instance type
        byte type = -1;
        type = config.getCodecType(v);
        if (type == -1) {
            throw new CodecException("不可被序列化为java.util.Map的类型(" + v.getClass() + ")");
        }
        output.writeByte(type);

        try {
            Map<?, ?> map = (Map<?, ?>) v;
            int length = map.size();
            output.writeVInt(length);

            Object key = null;
            Object value = null;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                key = entry.getKey();
                value = entry.getValue();

                // write key
                config.lookup(key).encode(output, key, config);

                // write value
                if (value == null) {
                    output.writeByte(CodecType.NULL);
                } else {
                    output.writeByte(CodecType.NOT_NULL);
                    config.lookup(value).encode(output, value, config);
                }
            }
        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

    public Map<Object, Object> decode(DataInput input, CodecConfig config) throws CodecException {
        try {
            byte type = input.readByte();
            Map map = (Map) config.getClassByType(type).newInstance();

            int length = input.readVInt();
            Object key, value;
            Class<?> kClass, vClass;
            for (int i = 0; i < length; i++) {
                // read key
                type = input.readByte();
                key = config.lookup(type).decode(input, config);

                // read value
                if (input.readByte() == CodecType.NULL) {
                    value = null;
                } else {
                    type = input.readByte();
                    value = config.lookup(type).decode(input, config);
                }

                map.put(key, value);
            }

            return map;
        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
