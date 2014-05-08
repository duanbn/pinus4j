package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class BooleanCodec implements Codec<Boolean>
{

    @Override
    public void encode(DataOutput output, Boolean v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_OBOOLEAN);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeBoolean(v);
        }
    }

    @Override
    public Boolean decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readBoolean();
    }

}
