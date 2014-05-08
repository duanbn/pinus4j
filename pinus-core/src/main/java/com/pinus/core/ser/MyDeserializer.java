package com.pinus.core.ser;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.*;
import com.pinus.core.ser.codec.exception.CodecException;
import com.pinus.core.util.GzipCompressUtil;

/**
 * 反序列化工具类，解析一个二进制的字节数组转化成一个Java对象.
 * 读取编码标志位确定转化为那个对象.
 *
 * @author duanbn
 * @see CodecConfig
 */
public class MyDeserializer implements Deserializer
{
    private static final ThreadLocal<DataInput> inputRef = new ThreadLocal<DataInput>();

    private static MyDeserializer instance;

    private CodecConfig config;

    private MyDeserializer()
    {
        this.config = CodecConfig.load();
    }

    public static MyDeserializer getInstance()
    {
        if (instance == null) {
            synchronized (MyDeserializer.class) {
                if (instance == null) {
                    instance = new MyDeserializer();
                }
            }
        }

        return instance;
    }

    @Override
    public Object deser(byte[] b, boolean isCompress) throws DeserializeException
    {
        if (b == null || b.length == 0) {
            throw new IllegalArgumentException("b=null");
        }

        try {
            // memory leak
            /*
            DataInput input = _getInput();
            if (isCompress) {
                input.setDataBuffer(GzipCompressUtil.uncompress(b));
            } else {
                input.setDataBuffer(b);
            }
            */
            DataInput input = null;
            if (isCompress) {
                input = new ByteBufferInput(GzipCompressUtil.uncompress(b));
            } else {
                input = new ByteBufferInput(b);
            }

            byte type = input.readByte();
            Codec codec = config.lookup(type);

            return codec.decode(input, config);
        } catch (Exception e) {
            throw new DeserializeException(e);
        }
    }

    @Override
    public Object deser(byte[] b) throws DeserializeException
    {
        return deser(b, true);
    }

    @Override
    public <T> T deser(byte[] b, boolean isCompress, Class<T> T) throws DeserializeException
    {
        return (T) deser(b, isCompress);
    }
    
    @Override
    public <T> T deser(byte[] b, Class<T> T) throws DeserializeException
    {
        return deser(b, true, T);
    }

    private DataInput _getInput()
    {
        if (inputRef.get() == null) {
            inputRef.set(new ByteBufferInput());
        }

        return inputRef.get();
    }
}
