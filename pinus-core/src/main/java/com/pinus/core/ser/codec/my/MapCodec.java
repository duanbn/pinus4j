package com.pinus.core.ser.codec.my;

import java.util.*;
import java.io.IOException;
import java.util.zip.*;
import java.lang.reflect.*;

import com.pinus.core.ser.codec.*;
import com.pinus.core.ser.codec.exception.CodecException;
import com.pinus.core.io.*;

/**
 * @author duanbn
 */
public class MapCodec implements Codec<Map<Object, Object>>
{

    public void encode(DataOutput output, Map<Object, Object> v, CodecConfig config) throws CodecException
    {
        output.writeByte(CodecType.TYPE_MAP);

        // write map instance type
        byte type = -1;
        type = config.getCodecType(v);
        if (type == -1) {
            throw new CodecException("不可被序列化为java.util.Map的类型(" + v.getClass() + ")");
        }
        output.writeByte(type);

        try {
            Map<?,?> map = (Map<?,?>) v;
            int length = map.size();
            output.writeVInt(length);

            boolean isWriteType = true;
            Object key = null;
            Object value = null;
            for (Map.Entry<?,?> entry : map.entrySet()) {
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
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

    public Map<Object, Object> decode(DataInput input, CodecConfig config) throws CodecException
    {
        try {
            byte type = input.readByte();
            Map map = (Map) config.getClassByType(type).newInstance();

            int length = input.readVInt();
            Object key, value;
            Class<?> kClass, vClass;
            for (int i=0; i<length; i++) {
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
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
