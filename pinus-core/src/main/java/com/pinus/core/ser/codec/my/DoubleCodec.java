package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class DoubleCodec implements Codec<Double>
{

    @Override
    public void encode(DataOutput output, Double v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_ODOUBLE);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeDouble(v);
        }
    }

    @Override
    public Double decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readDouble();
    }

}
