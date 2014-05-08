package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

public class BooleanArrayCodec implements Codec<boolean[]>
{

    @Override
    public void encode(DataOutput output, boolean[] v, CodecConfig config)
    {
        // write type
        output.writeByte(CodecType.TYPE_ARRAY_BOOLEAN);

        // write is null
        if (v == null) {
            output.writeByte(CodecType.NULL);
            return;
        }
        output.writeByte(CodecType.NOT_NULL);

        // write length
        int length = v.length;
        output.writeVInt(length);

        // write value
        for (int i=0; i<length; i++) {
            output.writeBoolean(v[i]);
        }
    }

    @Override
    public boolean[] decode(DataInput input, CodecConfig config)
    {
        // read is null
        if (input.readByte() == CodecType.NULL) {
            return null;
        }

        // read length
        int length = input.readVInt();
        boolean[] array = new boolean[length];
        for (int i=0; i<length; i++) {
            array[i] = input.readBoolean();
        }
        return array;
    }

}
