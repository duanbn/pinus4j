package com.pinus.core.ser;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import com.pinus.core.ser.codec.*;
import com.pinus.core.io.*;
import com.pinus.core.ser.codec.exception.CodecException;
import com.pinus.core.util.GzipCompressUtil;

/**
 * 负责将一个对象进行序列化，可以被序列化的类型请参考CodecConfig类.
 * 二进制格式遵循,  CodecType_dataByte.
 *
 * @see CodecConfig
 */
public class MySerializer implements Serializer
{

    private static final ThreadLocal<DataOutput> outputRef = new ThreadLocal<DataOutput>();

    private static MySerializer instance;

    private CodecConfig config;

    private MySerializer()
    {
        this.config = CodecConfig.load();
    }

    public static MySerializer getInstance()
    {
        if (instance == null) {
            synchronized (MySerializer.class) {
                if (instance == null) {
                    instance = new MySerializer();
                }
            }
        }

        return instance;
    }

    @Override
    public byte[] ser(Object v) throws SerializeException
    {
        return ser(v, true);
    }

    @Override
    public byte[] ser(Object v, boolean isCompress) throws SerializeException
    {
        try {
            // memory leak
            //DataOutput output = _getOutput();
            DataOutput output = new ByteBufferOutput();

            config.lookup(v).encode(output, v, config);

            if (isCompress)
                return GzipCompressUtil.compress(output.byteArray());
                
            return output.byteArray();
        } catch (Exception e) {
            throw new SerializeException(e);
        }
    }

    private DataOutput _getOutput()
    {
        if (outputRef.get() == null) {
            outputRef.set(new ByteBufferOutput());
        }

        return outputRef.get();
    }

}
