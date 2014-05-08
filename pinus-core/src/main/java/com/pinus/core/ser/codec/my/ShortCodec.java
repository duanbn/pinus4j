package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class ShortCodec implements Codec<Short>
{

    @Override
    public void encode(DataOutput output, Short v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_OSHORT);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeShort(v);
        }
    }

    @Override
    public Short decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readShort();
    }

}
