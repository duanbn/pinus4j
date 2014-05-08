package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class StringCodec implements Codec<String>
{

    @Override
    public void encode(DataOutput output, String v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_STRING);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeGBK(v);
        }
    }

    @Override
    public String decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readGBK();
    }

}
