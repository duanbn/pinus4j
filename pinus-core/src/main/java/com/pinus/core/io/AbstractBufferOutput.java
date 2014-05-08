package com.pinus.core.io;

import java.io.*;

public abstract class AbstractBufferOutput implements DataOutput {

    public static final int UNI_SUR_HIGH_START = 0xD800;
    public static final int UNI_SUR_LOW_START = 0xDC00;

    private static final long HALF_SHIFT = 10;

    private static final int SURROGATE_OFFSET = 
        Character.MIN_SUPPLEMENTARY_CODE_POINT - 
        (UNI_SUR_HIGH_START << HALF_SHIFT) - UNI_SUR_LOW_START;

    protected int offset;
    /**
     * total capacity.
     */
    protected int limit;

    @Override
    public void writeBoolean(boolean b)
    {
        _write(b ? (byte)1 : (byte)0);
    }

    @Override
    public void writeByte(byte b)
    {
        _write(b);
    }

    public void writeChar(char c)
    {
        int code = (int) c;
        writeVInt(code);
    }

    @Override
    public void writeShort(short s)
    {
        writeVInt(Short.valueOf(s).intValue());
    }

    @Override
    public void writeInt(int i)
    {
        _write((byte)((i >>> 24) & 0xFF));
        _write((byte)((i >>> 16) & 0xFF));
        _write((byte)((i >>> 8) & 0xFF));
        _write((byte)((i >>> 0) & 0xFF));
    }

    @Override
    public void writeLong(long l)
    {
        _write((byte)(l >>> 56));
        _write((byte)(l >>> 48));
        _write((byte)(l >>> 40));
        _write((byte)(l >>> 32));
        _write((byte)(l >>> 24));
        _write((byte)(l >>> 16));
        _write((byte)(l >>> 8));
        _write((byte)(l >>> 0));
    }

    @Override
    public void writeFloat(float f)
    {
        writeVInt(Float.floatToIntBits(f));
    }

    @Override
    public void writeDouble(double d)
    {
        writeVLong(Double.doubleToLongBits(d));
    }

    @Override
    public void writeVInt(int i)
    {
        while ((i & ~0x7F) != 0) {
            writeByte((byte)((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        writeByte((byte)i);
    }

    @Override
    public void writeVLong(long i)
    {
        while ((i & ~0x7FL) != 0L) {
            writeByte((byte)((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        writeByte((byte)i);
    }

    @Override
    public void writeUTF8(String str)
    {
        writeUTF8(str, 0, str.length());
    }

    @Override
    public void writeUTF8(String str, int offset, int length)
    {
        byte[] sbyte = new byte[length * 4];

        int end = offset + length;
        if (end > str.length())
            throw new IllegalArgumentException("offset + length > str.length()");

        int count = 0;
        int code = 0;
        for (int i=0; i<end; i++) {
            code = (int)str.charAt(i);
            if (code < 0x80) {
                sbyte[count++] = (byte)code;
            } else if (code < 0x800) {
                sbyte[count++] = (byte)(0xC0 | ((code >> 6) & 0x1F));
                sbyte[count++] = (byte)(0x80 | (code & 0x3F));
            } else if (code < 0xD800 || code > 0xDFFF) {
                sbyte[count++] = (byte)(0xE0 | ((code >> 12) & 0x0F));
                sbyte[count++] = (byte)(0x80 | ((code >> 6) & 0x3F));
                sbyte[count++] = (byte)(0x80 | (code & 0x3F));
            } else {
                if (code < 0xDC00 && (i < end-1)) {
                    int utf32 = (int) str.charAt(i+1);
                    if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
                        utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
                        i++;
                        sbyte[count++] = (byte)(0xF0 | (utf32 >> 18));
                        sbyte[count++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
                        sbyte[count++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
                        sbyte[count++] = (byte)(0x80 | (utf32 & 0x3F));
                        continue;
                    }
                }
                sbyte[count++] = (byte) 0xEF;
                sbyte[count++] = (byte) 0xBF;
                sbyte[count++] = (byte) 0xBD;
            }
        }
        
        writeVInt(count);
        write(sbyte, 0, count);
    }

    @Override
    public void writeGBK(String s) {
        try {
            byte[] b = s.getBytes("gbk");
            writeVInt(b.length);
            write(b, 0, b.length);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size()
    {
        return this.offset;
    }

    @Override
    public int limit()
    {
        return this.limit;
    }

    @Override
    public void clean()
    {
        reset();
    }

    @Override
    public void write(byte[] b, int offset, int length)
    {
        int end = offset + length;
        if (end > b.length)
            throw new IllegalStateException("offset + length > b.length");

        for (int i=offset; i<end; i++) {
            _write(b[i]);
        }
    }

    protected abstract void _write(byte b);

    protected abstract void _allocMore();

}
