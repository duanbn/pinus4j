package com.pinus.core.ser.codec.my;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.DataOutput;
import com.pinus.core.io.DataInput;

public class ExceptionCodec implements Codec<Throwable> {

    @Override
    public void encode(DataOutput output, Throwable v, CodecConfig config)
    {
        output.writeByte(CodecType.TYPE_EXCEPTION);

        if (v == null) {
            output.writeByte(CodecType.NULL);
        } else {
            output.writeByte(CodecType.NOT_NULL);
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(v);
                byte[] b = baos.toByteArray();
                oos.close();
                baos.close();

                output.writeVInt(b.length);
                output.write(b, 0, b.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Throwable decode(DataInput input, CodecConfig config)
    {
        byte isNull = input.readByte();
        if (isNull == CodecType.NULL) {
            return null;
        }
        try {
            byte[] tb = new byte[input.readVInt()];
            input.read(tb, 0, tb.length);

            ByteArrayInputStream bais = new ByteArrayInputStream(tb);
            ObjectInputStream ois = new ObjectInputStream(bais);

            Throwable t = (Throwable) ois.readObject();

            ois.close();
            bais.close();

            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
