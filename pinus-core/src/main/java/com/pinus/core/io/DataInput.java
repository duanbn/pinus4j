package com.pinus.core.io;

/**
 * 数据输入.
 * Java基本类型的数据输入
 *
 * @author duanbn
 * @since 1.0
 */
public interface DataInput
{
    boolean readBoolean();

    byte readByte();

    char readChar();

    short readShort();

    int readInt();

    long readLong();

    float readFloat();

    double readDouble();

    int readVInt();

    long readVLong();

    String readUTF8();

    String readGBK();

    int remain();

    void read(byte[] b, int offset, int length);

    void setDataBuffer(byte[] b);

}
