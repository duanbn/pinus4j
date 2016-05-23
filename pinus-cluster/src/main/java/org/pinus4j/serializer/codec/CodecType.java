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

package org.pinus4j.serializer.codec;

public class CodecType {
	public static final byte NULL = 0x00;
	public static final byte NOT_NULL = 0x01;
	public static final byte END = 0x02;

	public static final byte TYPE_EXCEPTION = 0x03;
	public static final byte TYPE_CLASS = 0x04;

	public static final byte TYPE_BOOLEAN = 0x05;
	public static final byte TYPE_OBOOLEAN = 0x06;
	public static final byte TYPE_ARRAY_BOOLEAN = 0x07;
	public static final byte TYPE_ARRAY_OBOOLEAN = 0x08;

	public static final byte TYPE_BYTE = 0x09;
	public static final byte TYPE_OBYTE = 0x10;
	public static final byte TYPE_ARRAY_BYTE = 0x11;
	public static final byte TYPE_ARRAY_OBYTE = 0x12;

	public static final byte TYPE_CHAR = 0x13;
	public static final byte TYPE_OCHAR = 0x14;
	public static final byte TYPE_ARRAY_CHAR = 0x15;
	public static final byte TYPE_ARRAY_OCHAR = 0x16;

	public static final byte TYPE_SHORT = 0x17;
	public static final byte TYPE_OSHORT = 0x18;
	public static final byte TYPE_ARRAY_SHORT = 0x19;
	public static final byte TYPE_ARRAY_OSHORT = 0x20;

	public static final byte TYPE_INT = 0x21;
	public static final byte TYPE_OINT = 0x22;
	public static final byte TYPE_ARRAY_INT = 0x23;
	public static final byte TYPE_ARRAY_OINT = 0x24;

	public static final byte TYPE_LONG = 0x25;
	public static final byte TYPE_OLONG = 0x26;
	public static final byte TYPE_ARRAY_LONG = 0x027;
	public static final byte TYPE_ARRAY_OLONG = 0x28;

	public static final byte TYPE_FLOAT = 0x29;
	public static final byte TYPE_OFLOAT = 0x30;
	public static final byte TYPE_ARRAY_FLOAT = 0x31;
	public static final byte TYPE_ARRAY_OFLOAT = 0x32;

	public static final byte TYPE_DOUBLE = 0x33;
	public static final byte TYPE_ODOUBLE = 0x34;
	public static final byte TYPE_ARRAY_DOUBLE = 0x35;
	public static final byte TYPE_ARRAY_ODOUBLE = 0x36;

	public static final byte TYPE_OBJECT = 0x37;
	public static final byte TYPE_ARRAY_OBJECT = 0x38;

	public static final byte TYPE_DATE = 0x39;
	public static final byte TYPE_ARRAY_DATE = 0x40;

	public static final byte TYPE_CALENDER = 0x41;
	public static final byte TYPE_ARRAY_CALENDER = 0x42;

	public static final byte TYPE_STRING = 0x43;
	public static final byte TYPE_ARRAY_STRING = 0x44;

	public static final byte TYPE_LIST = 0x45;
	public static final byte TYPE_ARRAYLIST = 0x46;
	public static final byte TYPE_LINKEDLIST = 0x47;
	public static final byte TYPE_COPYONWRITEARRAYLIST = 0x48;

	public static final byte TYPE_SET = 0x49;
	public static final byte TYPE_HASHSET = 0x50;
	public static final byte TYPE_TREESET = 0x51;
	public static final byte TYPE_LINKEDHASHSET = 0x52;

	public static final byte TYPE_MAP = 0x53;
	public static final byte TYPE_HASHMAP = 0x54;
	public static final byte TYPE_TREEMAP = 0x55;
	public static final byte TYPE_CONCURRENTHASHMAP = 0x56;
	public static final byte TYPE_LINKEDHASHMAP = 0x57;

	public static final byte TYPE_ENUM = 0x58;
	public static final byte TYPE_ARRAY_ENUM = 0x59;

	public static final byte TYPE_TIMESTAMP = 0x60;
	public static final byte TYPE_ARRAY_TIMESTAMP = 0x61;

	public static final byte TYPE_SQLDATE = 0x62;
	public static final byte TYPE_ARRAY_SQLDATE = 0x63;

}
