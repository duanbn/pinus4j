package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class ClassCodec implements Codec<Class>
{

    @Override
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

    @Override
    public Class decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class c = loader.loadClass(input.readUTF8());
            return c;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
