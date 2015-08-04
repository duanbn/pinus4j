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

package org.pinus4j.serializer;

import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.io.ByteBufferOutput;
import org.pinus4j.serializer.io.DataOutput;
import org.pinus4j.utils.GzipCompressUtil;

/**
 * 负责将一个对象进行序列化，可以被序列化的类型请参考CodecConfig类. 二进制格式遵循, CodecType_dataByte.
 * 
 * @see CodecConfig
 */
public class MySerializer implements Serializer {

    private static final ThreadLocal<DataOutput> outputRef = new ThreadLocal<DataOutput>();

    private static MySerializer                  instance;

    private CodecConfig                          config;

    private MySerializer() {
        this.config = CodecConfig.load();
    }

    public static MySerializer getInstance() {
        if (instance == null) {
            synchronized (MySerializer.class) {
                if (instance == null) {
                    instance = new MySerializer();
                }
            }
        }

        return instance;
    }

    public byte[] ser(Object v) throws SerializeException {
        return ser(v, false);
    }

    public byte[] ser(Object v, boolean isCompress) throws SerializeException {
        try {
            // memory leak
            // DataOutput output = _getOutput();
            DataOutput output = new ByteBufferOutput();

            Codec codec = config.lookup(v);
            codec.encode(output, v, config);

            if (isCompress) {
                byte[] data = GzipCompressUtil.compress(output.byteArray());
                return data;
            }

            return output.byteArray();
        } catch (Exception e) {
            throw new SerializeException(e);
        }
    }

    private DataOutput _getOutput() {
        if (outputRef.get() == null) {
            outputRef.set(new ByteBufferOutput());
        }

        return outputRef.get();
    }

}
