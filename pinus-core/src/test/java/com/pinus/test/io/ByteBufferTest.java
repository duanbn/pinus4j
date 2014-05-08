package com.pinus.test.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import com.pinus.core.io.ByteBufferInput;
import com.pinus.core.io.ByteBufferOutput;
import com.pinus.core.io.DataInput;
import com.pinus.core.io.DataOutput;
import com.pinus.test.BaseTest;

public class ByteBufferTest extends BaseTest
{
    private DataOutput output;
    private ObjectMapper mapper;

    @Before
    public void setUp()
    {
        output = new ByteBufferOutput();
        mapper = new ObjectMapper();
    }

    @Test
    public void testSpeed() {
        String s = genWord(5000);
        output.writeGBK(s);
        byte[] rst = output.byteArray();
    }

    @Test
    public void remain()
    {
        try {
            output.writeVInt(100000);
            output.writeVInt(100000);
            output.writeVInt(1);
            System.out.println(output.size());
            DataInput input = new ByteBufferInput(output.byteArray());
            input.readVInt();
            System.out.println(input.remain());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testByte()
    {
        output.writeByte((byte)97);
        DataInput input = new ByteBufferInput(output.byteArray());
        byte b = input.readByte();
    }

    @Test
    public void testChar()
    {
        output.writeChar('今');
        byte[] b = output.byteArray();
        DataInput input = new ByteBufferInput(b);
        char c = input.readChar();
        System.out.println(c);
    }

    @Test
    public void testVInt()
    {
        try {
            output.writeVInt(2);
            
            byte[] b = output.byteArray();

            DataInput input = new ByteBufferInput(b);
            int i = input.readVInt();
            System.out.println(i);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testVLong()
    {
        try {
            output.writeVLong(100000);
            output.writeVLong(100000);
            byte[] b = output.byteArray();
            System.out.println(output.size() + ":" + b.length);

            DataInput input = new ByteBufferInput(b);
            long i = input.readVLong();
            System.out.println(i);
            i = input.readVLong();
            System.out.println(i);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNumber()
    {
        output.writeShort((short)10);
        output.writeInt(100000);
        output.writeLong(Long.MAX_VALUE);
        byte[] b = output.byteArray();
        DataInput input = new ByteBufferInput(b);
        short s = input.readShort();
        System.out.println(s);
        int i = input.readInt();
        System.out.println(i);
        long l = input.readLong();
        System.out.println(l);
    }

    @Test
    public void testFloat()
    {
        output.writeFloat(1.1f);
        output.writeDouble(2.2);
        byte[] b = output.byteArray();
        DataInput input = new ByteBufferInput(b);
        float f = input.readFloat();
        double d = input.readDouble();
        System.out.println(f);
        System.out.println(d);
    }

    @Test
    public void testString()
    {
        try {
            String s = "a";

            output.writeUTF8(s);
            byte[] b = output.byteArray();
            showLength("utf encoding", b);

            output.clean();
            output.writeGBK(s);
            b = output.byteArray();
            showLength("gbk encoding", b);
            DataInput input = new ByteBufferInput(output.byteArray());
            String s1 = input.readGBK();
            System.out.println(s1);

            b = mapper.writeValueAsBytes(s);
            showLength("jackson encoding", b);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteUTF()
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF("中华人民共和国");
            System.out.println(baos.toByteArray().length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

}
