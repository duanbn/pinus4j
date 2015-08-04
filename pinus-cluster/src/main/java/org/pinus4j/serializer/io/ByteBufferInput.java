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

package org.pinus4j.serializer.io;

import java.nio.charset.Charset;

public class ByteBufferInput implements DataInput {

    public static final int DEFAULT_BUF = 2048;

    private byte[]          _buf;
    private int             pos;

    public ByteBufferInput() {
        this(new byte[DEFAULT_BUF]);
    }

    public ByteBufferInput(byte[] buf) {
        _buf = buf;
    }

    public boolean readBoolean() {
        return _buf[pos++] == 0 ? false : true;
    }

    public byte readByte() {
        return _buf[pos++];
    }

    public char readChar() {
        int code = readVInt();
        return (char) code;
    }

    public short readShort() {
        return Integer.valueOf(readVInt()).shortValue();
    }

    public int readInt() {
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16) | ((readByte() & 0xFF) << 8)
                | (readByte() & 0xFF);
    }

    public long readLong() {
        return (((long) _buf[pos++] << 56) + ((long) (_buf[pos++] & 0xFF) << 48) + ((long) (_buf[pos++] & 0xFF) << 40)
                + ((long) (_buf[pos++] & 0xFF) << 32) + ((long) (_buf[pos++] & 0xFF) << 24)
                + ((_buf[pos++] & 0xFF) << 16) + ((_buf[pos++] & 0xFF) << 8) + ((_buf[pos++] & 0xFF) << 0));
    }

    public float readFloat() {
        return Float.intBitsToFloat(readVInt());
    }

    public double readDouble() {
        return Double.longBitsToDouble(readVLong());
    }

    public int readVInt() {
        byte b = readByte();
        if (b >= 0)
            return b;
        int i = b & 0x7F;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7F) << 14;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7F) << 21;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0)
            return i;
        throw new RuntimeException("读取变长整型数错误,二进制格式有误");
    }

    public long readVLong() {
        byte b = readByte();
        if (b >= 0)
            return b;
        long i = b & 0x7FL;
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0)
            return i;
        b = readByte();
        i |= (b & 0x7FL) << 56;
        if (b >= 0)
            return i;
        throw new RuntimeException("读取变长长整型数错误,二进制格式有误");
    }

    public String readUTF8() {
        int length = readVInt();
        final byte[] bytes = new byte[length];
        read(bytes, 0, length);
        return new String(bytes, 0, length, Charset.forName("UTF-8"));
    }

    public String readGBK() {
        int length = readVInt();
        final byte[] bytes = new byte[length];
        read(bytes, 0, length);
        return new String(bytes, 0, length, Charset.forName("GBK"));
    }

    public int remain() {
        return _buf.length - pos;
    }

    public void read(byte[] bytes, int offset, int length) {
        System.arraycopy(_buf, pos, bytes, 0, length);
        pos += length;
    }

    public void setDataBuffer(byte[] b) {
        this._buf = b;
        this.pos = 0;
    }

}
