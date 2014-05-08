package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class LongCodec implements Codec<Long>
{

    @Override
    public void encode(DataOutput output, Long v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_OLONG);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeLong(v);
        }
    }

    @Override
    public Long decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readLong();
    }

}
