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
public class ListCodec implements Codec<List<Object>>
{

    @Override
    public void encode(DataOutput output, List<Object> v, CodecConfig config) throws CodecException
    {
        output.writeByte(CodecType.TYPE_LIST);

        // write list instance type
        byte type = -1;
        type = config.getCodecType(v);
        if (type == -1) {
            throw new CodecException("不可被序列化为List的类型(" + v.getClass() + ")");
        }
        output.writeByte(type);

        try {
            List<?> list = (List<?>) v;
            int length = list.size();
            output.writeVInt(length); // write length

            for (Object obj : list) {
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
    public List<Object> decode(DataInput input, CodecConfig config) throws CodecException
    {
        try {
            byte type = input.readByte();

            List list = (List) config.getClassByType(type).newInstance();

            int length = input.readVInt(); // read length

            for (int i=0; i<length; i++) {
                if (input.readByte() != CodecType.NULL) {
                    type = input.readByte();
                    list.add(config.lookup(type).decode(input, config));
                }
            }
            
            return list;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
