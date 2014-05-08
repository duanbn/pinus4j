package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;
import com.pinus.core.util.ReflectUtil;
import com.pinus.core.ser.codec.exception.*;

public class EnumCodec implements Codec<Enum>
{

    @Override
    public void encode(DataOutput output, Enum v, CodecConfig config) throws CodecException
    {

        output.writeByte(CodecType.TYPE_ENUM);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeGBK(v.getDeclaringClass().getName());
            output.writeGBK(v.name());
        }

    }

    @Override
    public Enum decode(DataInput input, CodecConfig config) throws CodecException
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        
        Class<Enum> oc = (Class<Enum>) ReflectUtil.getClass(input.readGBK());
        String name = input.readGBK();
        return Enum.valueOf(oc, name);
    }

}
