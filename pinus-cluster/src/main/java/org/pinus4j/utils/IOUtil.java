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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.serializer.DeserializeException;
import org.pinus4j.serializer.Deserializer;
import org.pinus4j.serializer.MyDeserializer;
import org.pinus4j.serializer.MySerializer;
import org.pinus4j.serializer.SerializeException;
import org.pinus4j.serializer.Serializer;

/**
 * input output utility.
 * 
 * @author duanbn
 */
public class IOUtil {

    private static Serializer   ser   = MySerializer.getInstance();

    private static Deserializer deser = MyDeserializer.getInstance();

    public static byte[] getBytesByJava(Object obj) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;

        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (baos != null) {
                    baos.close();
                }
                if (oos != null) {
                    oos.close();
                }
            } catch (IOException e) {
            }
        }

        return baos.toByteArray();
    }

    public static <T> T getObjectByJava(byte[] data, Class<T> clazz) {
        if (data == null)
            return null;

        ObjectInputStream ois = null;
        ByteArrayInputStream bais = null;
        try {
            bais = new ByteArrayInputStream(data);
            ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (bais != null) {
                    bais.close();
                }
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {
            }
        }
    }

    public static byte[] getBytes(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        try {
            return ser.ser(obj);
        } catch (SerializeException e) {
            throw new DBOperationException(e);
        }
    }

    public static <T> T getObject(byte[] data, Class<T> clazz) {
        if (data == null)
            return null;

        try {
            return deser.deser(data, clazz);
        } catch (DeserializeException e) {
            throw new DBOperationException(e);
        }
    }

}
