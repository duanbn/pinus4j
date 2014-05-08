package com.pinus.test;

import java.util.*;
import java.io.*;
import java.util.zip.*;

import org.codehaus.jackson.map.*;
import com.alibaba.fastjson.JSON;

public class BaseTest
{
    protected char[] words = new char[] {'第', '提', '去', '额', '我', 'a', '速', '而', '的', '分', '平', '吗', '库', '你'};
    protected Random r = new Random();

    protected String genWord(int count)
    {
        int length = words.length;
        char[] text = new char[count];
        for (int i=0; i<count; i++) {
            text[i] = words[r.nextInt(length)];
        }

        return new String(text);
    }

    protected byte[] writeFjson(Object o)  throws Exception {
        return JSON.toJSONString(o).getBytes();
    }

    protected <T> T readFjson(byte[] b, Class<T> clazz) throws Exception {
        String s = new String(b);
        return (T) JSON.parseObject(s, clazz);
    }

    ObjectMapper mapper = new ObjectMapper();
    protected byte[] writeJson(Object o) throws Exception
    {
        return mapper.writeValueAsBytes(o);
    }

    protected <T> T readJson(byte[] b, Class<T> clazz) throws Exception
    {
        return (T) mapper.readValue(b, clazz);
    }

    protected byte[] writeObject(Object o) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        oos.writeObject(o);

        byte[] b = baos.toByteArray();
        baos.close();

        return b;
    }

    protected Object readObject(byte[] b) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);

        Object obj = ois.readObject();
        bais.close();

        return obj;
    }

    protected byte[] gzip(byte[] b) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(baos);
        gzip.write(b, 0, b.length);
        gzip.finish();
        byte[] gdata = baos.toByteArray();
        baos.close();

        return gdata;
    }

    protected void showLength(String desc, byte[] b) {
        System.out.println(desc + " serialize length: " + b.length);
    }

    protected void showLength(byte[] b)
    {
        System.out.println("serialize length: " + b.length);
    }

}
