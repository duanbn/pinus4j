package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class FloatCodec implements Codec<Float>
{

    @Override
    public void encode(DataOutput output, Float v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_OFLOAT);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeFloat(v);
        }
    }

    @Override
    public Float decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readFloat();
    }

}
