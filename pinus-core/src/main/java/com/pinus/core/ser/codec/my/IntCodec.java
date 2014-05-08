package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class IntCodec implements Codec<Integer>
{

    @Override
    public void encode(DataOutput output, Integer v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_OINT);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeInt(v);
        }
    }

    @Override
    public Integer decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readInt();
    }

}
