package com.pinus.core.ser.codec.my;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

public class DoubleArrayCodec implements Codec<double[]>
{

    @Override
    public void encode(DataOutput output, double[] v, CodecConfig config)
    {
        // write type
        output.writeByte(CodecType.TYPE_ARRAY_DOUBLE);

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
            output.writeDouble(v[i]);
        }
    }

    @Override
    public double[] decode(DataInput input, CodecConfig config)
    {
        // read is null
        if (input.readByte() == CodecType.NULL) {
            return null;
        }

        // read length
        int length = input.readVInt();
        double[] array = new double[length];
        for (int i=0; i<length; i++) {
            array[i] = input.readDouble();
        }
        return array;
    }

}
