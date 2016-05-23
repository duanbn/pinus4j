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

package org.pinus4j.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressUtil {

    private static final int COMPRESS_BUFFER = 1024 * 1;

    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(baos);

        gzip.write(data, 0, data.length);
        gzip.finish();

        byte[] gdata = baos.toByteArray();

        baos.close();
        gzip.close();

        return gdata;
    }

    public static byte[] uncompress(byte[] data) throws IOException {

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
