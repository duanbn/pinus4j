package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

/**
 * @author duanbn
 */
public class CharCodec implements Codec<Character>
{

    @Override
    public void encode(DataOutput output, Character v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_OCHAR);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            output.writeChar(v);
        }
    }

    @Override
    public Character decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        return input.readChar();
    }

}
