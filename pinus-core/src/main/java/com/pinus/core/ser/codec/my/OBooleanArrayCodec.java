package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

public class OBooleanArrayCodec implements Codec<Boolean[]>
{

    @Override
    public void encode(DataOutput output, Boolean[] v, CodecConfig config)
    {
        // write type
        output.writeByte(CodecType.TYPE_ARRAY_OBOOLEAN);

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
            if (v[i] == null) {
                output.writeByte(CodecType.NULL);
                continue;
            }
            output.writeByte(CodecType.NOT_NULL);
            output.writeBoolean(v[i]);
        }
    }

    @Override
    public Boolean[] decode(DataInput input, CodecConfig config)
    {
        // read is null
        if (input.readByte() == CodecType.NULL) {
            return null;
        }

        // read length
        int length = input.readVInt();
        Boolean[] array = new Boolean[length];
        for (int i=0; i<length; i++) {
            if (input.readByte() == CodecType.NOT_NULL) {
                array[i] = input.readBoolean();
            }
        }
        return array;
    }

}
