package com.pinus.core.util;

import java.io.*;
import java.util.zip.*;

public class GzipCompressUtil
{

    private static final int COMPRESS_BUFFER = 1024 * 1;

    public static byte[] compress(byte[] data) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(baos);

        gzip.write(data, 0, data.length);
        gzip.finish();

        byte[] gdata = baos.toByteArray();

        baos.close();
        gzip.close();

        return gdata;
    }

    public static byte[] uncompress(byte[] data) throws IOException
    {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        GZIPInputStream gzip = new GZIPInputStream(bais);

        byte[] buffer = new byte[COMPRESS_BUFFER];
        int n = 0;
        while ((n = gzip.read(buffer, 0, buffer.length)) >= 0) {
            baos.write(buffer, 0, n);
        }
        byte[] b = baos.toByteArray();

        bais.close();
        baos.close();
        gzip.close();

        return b;
    }

}
