package com.pinus.core.io;

/**
 * 数据输出接口.
 * Java基本类型的输出，输出到哪里由具体实现定义.
 *
 * @author duanbn
 * @since 1.0
 */
public interface DataOutput
{

    /**
     * 写Boolean类型.
     *
     * @param b Boolean值
     */
    void writeBoolean(boolean b);

    /**
     * 写Byte类型.
     *
     * @param b Byte值
     */
    void writeByte(byte b);

    void writeChar(char c);

    void writeShort(short s);

    void writeInt(int i);

    void writeLong(long l);

    void writeFloat(float f);

    void writeDouble(double d);
    
    void writeVInt(int i);

    void writeVLong(long l);

    void writeUTF8(String s);

    void writeUTF8(String s, int offset, int length);

    void writeGBK(String s);

    int size();

    int limit();

    void clean();

    void reset();

    void write(byte[] b, int offset, int length);

    /**
     * 返回输出数据的字节数组.
     *
     * @return 数据字节数组
     */
    byte[] byteArray();

}
