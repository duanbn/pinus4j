/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.serializer.codec.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;

public class ExceptionCodec implements Codec<Throwable> {

    public void encode(DataOutput output, Throwable v, CodecConfig config) {
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

    public Throwable decode(DataInput input, CodecConfig config) {
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
