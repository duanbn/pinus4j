package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

public class OByteArrayCodec implements Codec<Byte[]>
{

    @Override
    public void encode(DataOutput output, Byte[] v, CodecConfig config)
    {
        // write type
        output.writeByte(CodecType.TYPE_ARRAY_OBYTE);

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
            output.writeByte(v[i]);
        }
    }

    @Override
    public Byte[] decode(DataInput input, CodecConfig config)
    {
        // read is null
        if (input.readByte() == CodecType.NULL) {
            return null;
        }

        // read length
        int length = input.readVInt();
        Byte[] array = new Byte[length];
        for (int i=0; i<length; i++) {
            if (input.readByte() == CodecType.NOT_NULL) {
                array[i] = input.readByte();
            }
        }
        return array;
    }

}
