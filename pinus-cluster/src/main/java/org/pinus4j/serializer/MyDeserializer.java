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
import org.pinus4j.serializer.io.ByteBufferInput;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.utils.GzipCompressUtil;

/**
 * 反序列化工具类，解析一个二进制的字节数组转化成一个Java对象. 读取编码标志位确定转化为那个对象.
 * 
 * @author duanbn
 * @see CodecConfig
 */
public class MyDeserializer implements Deserializer {
	private static final ThreadLocal<DataInput> inputRef = new ThreadLocal<DataInput>();

	private static MyDeserializer instance;

	private CodecConfig config;

	private MyDeserializer() {
		this.config = CodecConfig.load();
	}

	public static MyDeserializer getInstance() {
		if (instance == null) {
			synchronized (MyDeserializer.class) {
				if (instance == null) {
					instance = new MyDeserializer();
				}
			}
		}

		return instance;
	}

	public Object deser(byte[] b, boolean isCompress) throws DeserializeException {
		if (b == null || b.length == 0) {
			throw new IllegalArgumentException("b=null");
		}

		try {
			// memory leak
			/*
			 * DataInput input = _getInput(); if (isCompress) {
			 * input.setDataBuffer(GzipCompressUtil.uncompress(b)); } else {
			 * input.setDataBuffer(b); }
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

	public <T> T deser(byte[] b, boolean isCompress, Class<T> T) throws DeserializeException {
		return (T) deser(b, isCompress);
	}

	public <T> T deser(byte[] b, Class<T> T) throws DeserializeException {
		return deser(b, false, T);
	}

	private DataInput _getInput() {
		if (inputRef.get() == null) {
			inputRef.set(new ByteBufferInput());
		}

		return inputRef.get();
	}
}
