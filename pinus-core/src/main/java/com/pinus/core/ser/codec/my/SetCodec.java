package com.pinus.core.ser.codec.my;

import java.util.*;
import java.util.concurrent.*;
import java.io.IOException;
import java.util.zip.*;
import java.lang.reflect.*;

import com.pinus.core.ser.codec.*;
import com.pinus.core.ser.codec.exception.CodecException;
import com.pinus.core.io.*;

/**
 * 列表编码解码.
 *
 * @author duanbn
 */
public class SetCodec implements Codec<Set<Object>>
{

    @Override
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

    @Override
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
