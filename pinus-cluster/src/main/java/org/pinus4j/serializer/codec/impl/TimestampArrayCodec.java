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

package org.pinus4j.serializer.codec.impl;

import java.sql.Timestamp;

import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.codec.Codec;
import org.pinus4j.serializer.codec.CodecConfig;
import org.pinus4j.serializer.codec.CodecType;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;

/**
 * 
 * @author duanbn
 * 
 */
public class TimestampArrayCodec implements Codec<Timestamp[]> {

	public void encode(DataOutput output, Timestamp[] v, CodecConfig config) throws CodecException {
		// write type
		output.writeByte(CodecType.TYPE_ARRAY_TIMESTAMP);

		// write is null
		if (v == null) {
			output.writeByte(CodecType.NULL);
			return;
		}
		output.writeByte(CodecType.NOT_NULL);

		// write length
		int length = v.length;
		output.writeVInt(length);

		// write value
		for (int i = 0; i < length; i++) {
			output.writeVLong(v[i].getTime());
		}
	}

	public Timestamp[] decode(DataInput input, CodecConfig config) throws CodecException {
		// read is null
		if (input.readByte() == CodecType.NULL) {
			return null;
		}

		// read length
		int length = input.readVInt();
		Timestamp[] array = new Timestamp[length];
		for (int i = 0; i < length; i++) {
			array[i] = new Timestamp(input.readVLong());
		}
		return array;
	}

}
