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

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;
import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.codec.impl.BooleanArrayCodec;
import org.pinus4j.serializer.codec.impl.BooleanCodec;
import org.pinus4j.serializer.codec.impl.ByteArrayCodec;
import org.pinus4j.serializer.codec.impl.ByteCodec;
import org.pinus4j.serializer.codec.impl.CalendarArrayCodec;
import org.pinus4j.serializer.codec.impl.CalendarCodec;
import org.pinus4j.serializer.codec.impl.CharArrayCodec;
import org.pinus4j.serializer.codec.impl.CharCodec;
import org.pinus4j.serializer.codec.impl.CharacterArrayCodec;
import org.pinus4j.serializer.codec.impl.ClassCodec;
import org.pinus4j.serializer.codec.impl.DateArrayCodec;
import org.pinus4j.serializer.codec.impl.DateCodec;
import org.pinus4j.serializer.codec.impl.DoubleArrayCodec;
import org.pinus4j.serializer.codec.impl.DoubleCodec;
import org.pinus4j.serializer.codec.impl.EnumArrayCodec;
import org.pinus4j.serializer.codec.impl.EnumCodec;
import org.pinus4j.serializer.codec.impl.ExceptionCodec;
import org.pinus4j.serializer.codec.impl.FloatArrayCodec;
import org.pinus4j.serializer.codec.impl.FloatCodec;
import org.pinus4j.serializer.codec.impl.IntArrayCodec;
import org.pinus4j.serializer.codec.impl.IntCodec;
import org.pinus4j.serializer.codec.impl.IntegerArrayCodec;
import org.pinus4j.serializer.codec.impl.ListCodec;
import org.pinus4j.serializer.codec.impl.LongArrayCodec;
import org.pinus4j.serializer.codec.impl.LongCodec;
import org.pinus4j.serializer.codec.impl.MapCodec;
import org.pinus4j.serializer.codec.impl.OBooleanArrayCodec;
import org.pinus4j.serializer.codec.impl.OByteArrayCodec;
import org.pinus4j.serializer.codec.impl.ODoubleArrayCodec;
import org.pinus4j.serializer.codec.impl.OFloatArrayCodec;
import org.pinus4j.serializer.codec.impl.OLongArrayCodec;
import org.pinus4j.serializer.codec.impl.OShortArrayCodec;
import org.pinus4j.serializer.codec.impl.ObjectArrayCodec;
import org.pinus4j.serializer.codec.impl.ObjectCodec;
import org.pinus4j.serializer.codec.impl.SetCodec;
import org.pinus4j.serializer.codec.impl.ShortArrayCodec;
import org.pinus4j.serializer.codec.impl.ShortCodec;
import org.pinus4j.serializer.codec.impl.SqlDateArrayCodec;
import org.pinus4j.serializer.codec.impl.SqlDateCodec;
import org.pinus4j.serializer.codec.impl.StringArrayCodec;
import org.pinus4j.serializer.codec.impl.StringCodec;
import org.pinus4j.serializer.codec.impl.TimestampArrayCodec;
import org.pinus4j.serializer.codec.impl.TimestampCodec;
import org.pinus4j.utils.BeansUtil;

/**
 * 编码类配置文件. 所以编码类都应该在这里进行配置，每一种编码类支持一种类型的编码.
 * 
 * @author duanbn
 * @since 1.1
 */
public class CodecConfig {

	public static final Logger log = Logger.getLogger(CodecConfig.class);

	/**
	 * 通过被序列化类型查找编码类.
	 */
	public final Map<Class<?>, Codec> classCodecPool = new HashMap<Class<?>, Codec>();
	/**
	 * 通过被序列化类型查找编码标志位.
	 */
	public final Map<Class<?>, Byte> classCodecTypePool = new HashMap<Class<?>, Byte>();
	/**
	 * 通过编码标志位查找编码类.
	 */
	public final Map<Byte, Codec> codecTypeCodecPool = new HashMap<Byte, Codec>();
	/**
	 * 通过编码标志位查找被被序列化类型.
	 */
	public final Map<Byte, Class<?>> codecTypeClassPool = new HashMap<Byte, Class<?>>();

	public final ConfigItem[] config = new ConfigItem[] {

	new ConfigItem(Throwable.class, CodecType.TYPE_EXCEPTION, new ExceptionCodec()),
			new ConfigItem(Class.class, CodecType.TYPE_CLASS, new ClassCodec()),

			new ConfigItem(Boolean.TYPE, CodecType.TYPE_BOOLEAN, new BooleanCodec()),
			new ConfigItem(Boolean.class, CodecType.TYPE_OBOOLEAN, new BooleanCodec()),
			new ConfigItem(boolean[].class, CodecType.TYPE_ARRAY_BOOLEAN, new BooleanArrayCodec()),
			new ConfigItem(Boolean[].class, CodecType.TYPE_ARRAY_OBOOLEAN, new OBooleanArrayCodec()),

			new ConfigItem(Byte.TYPE, CodecType.TYPE_BYTE, new ByteCodec()),
			new ConfigItem(Byte.class, CodecType.TYPE_OBYTE, new ByteCodec()),
			new ConfigItem(byte[].class, CodecType.TYPE_ARRAY_BYTE, new ByteArrayCodec()),
			new ConfigItem(Byte[].class, CodecType.TYPE_ARRAY_OBYTE, new OByteArrayCodec()),

			new ConfigItem(Character.TYPE, CodecType.TYPE_CHAR, new CharCodec()),
			new ConfigItem(Character.class, CodecType.TYPE_OCHAR, new CharCodec()),
			new ConfigItem(char[].class, CodecType.TYPE_ARRAY_CHAR, new CharArrayCodec()),
			new ConfigItem(Character[].class, CodecType.TYPE_ARRAY_OCHAR, new CharacterArrayCodec()),

			new ConfigItem(Short.TYPE, CodecType.TYPE_SHORT, new ShortCodec()),
			new ConfigItem(Short.class, CodecType.TYPE_OSHORT, new ShortCodec()),
			new ConfigItem(short[].class, CodecType.TYPE_ARRAY_SHORT, new ShortArrayCodec()),
			new ConfigItem(Short[].class, CodecType.TYPE_ARRAY_OSHORT, new OShortArrayCodec()),

			new ConfigItem(Integer.TYPE, CodecType.TYPE_INT, new IntCodec()),
			new ConfigItem(Integer.class, CodecType.TYPE_OINT, new IntCodec()),
			new ConfigItem(int[].class, CodecType.TYPE_ARRAY_INT, new IntArrayCodec()),
			new ConfigItem(Integer[].class, CodecType.TYPE_ARRAY_OINT, new IntegerArrayCodec()),

			new ConfigItem(Long.TYPE, CodecType.TYPE_LONG, new LongCodec()),
			new ConfigItem(Long.class, CodecType.TYPE_OLONG, new LongCodec()),
			new ConfigItem(long[].class, CodecType.TYPE_ARRAY_LONG, new LongArrayCodec()),
			new ConfigItem(Long[].class, CodecType.TYPE_ARRAY_OLONG, new OLongArrayCodec()),

			new ConfigItem(Float.TYPE, CodecType.TYPE_FLOAT, new FloatCodec()),
			new ConfigItem(Float.class, CodecType.TYPE_OFLOAT, new FloatCodec()),
			new ConfigItem(float[].class, CodecType.TYPE_ARRAY_FLOAT, new FloatArrayCodec()),
			new ConfigItem(Float[].class, CodecType.TYPE_ARRAY_OFLOAT, new OFloatArrayCodec()),

			new ConfigItem(Double.TYPE, CodecType.TYPE_DOUBLE, new DoubleCodec()),
			new ConfigItem(Double.class, CodecType.TYPE_ODOUBLE, new DoubleCodec()),
			new ConfigItem(double[].class, CodecType.TYPE_ARRAY_DOUBLE, new DoubleArrayCodec()),
			new ConfigItem(Double[].class, CodecType.TYPE_ARRAY_ODOUBLE, new ODoubleArrayCodec()),

			new ConfigItem(String.class, CodecType.TYPE_STRING, new StringCodec()),
			new ConfigItem(String[].class, CodecType.TYPE_ARRAY_STRING, new StringArrayCodec()),

			new ConfigItem(Object.class, CodecType.TYPE_OBJECT, new ObjectCodec()),
			new ConfigItem(Object[].class, CodecType.TYPE_ARRAY_OBJECT, new ObjectArrayCodec()),

			new ConfigItem(Enum.class, CodecType.TYPE_ENUM, new EnumCodec()),
			new ConfigItem(Enum[].class, CodecType.TYPE_ARRAY_ENUM, new EnumArrayCodec()),

			new ConfigItem(List.class, CodecType.TYPE_LIST, new ListCodec()),
			new ConfigItem(ArrayList.class, CodecType.TYPE_ARRAYLIST, new ListCodec()),
			new ConfigItem(LinkedList.class, CodecType.TYPE_LINKEDLIST, new ListCodec()),
			new ConfigItem(CopyOnWriteArrayList.class, CodecType.TYPE_COPYONWRITEARRAYLIST, new ListCodec()),

			new ConfigItem(Set.class, CodecType.TYPE_SET, new SetCodec()),
			new ConfigItem(HashSet.class, CodecType.TYPE_HASHSET, new SetCodec()),
			new ConfigItem(TreeSet.class, CodecType.TYPE_TREESET, new SetCodec()),
			new ConfigItem(LinkedHashSet.class, CodecType.TYPE_LINKEDHASHSET, new SetCodec()),

			new ConfigItem(Map.class, CodecType.TYPE_MAP, new MapCodec()),
			new ConfigItem(HashMap.class, CodecType.TYPE_HASHMAP, new MapCodec()),
			new ConfigItem(ConcurrentHashMap.class, CodecType.TYPE_CONCURRENTHASHMAP, new MapCodec()),
			new ConfigItem(LinkedHashMap.class, CodecType.TYPE_LINKEDHASHMAP, new MapCodec()),

			new ConfigItem(Date.class, CodecType.TYPE_DATE, new DateCodec()),
			new ConfigItem(Date[].class, CodecType.TYPE_ARRAY_DATE, new DateArrayCodec()),
			new ConfigItem(java.sql.Date.class, CodecType.TYPE_SQLDATE, new SqlDateCodec()),
			new ConfigItem(java.sql.Date[].class, CodecType.TYPE_ARRAY_SQLDATE, new SqlDateArrayCodec()),

			new ConfigItem(Calendar.class, CodecType.TYPE_CALENDER, new CalendarCodec()),
			new ConfigItem(Calendar[].class, CodecType.TYPE_ARRAY_CALENDER, new CalendarArrayCodec()),
			new ConfigItem(GregorianCalendar.class, CodecType.TYPE_CALENDER, new CalendarCodec()),
			new ConfigItem(GregorianCalendar[].class, CodecType.TYPE_ARRAY_CALENDER, new CalendarArrayCodec()),

			new ConfigItem(Timestamp.class, CodecType.TYPE_TIMESTAMP, new TimestampCodec()),
			new ConfigItem(Timestamp[].class, CodecType.TYPE_ARRAY_TIMESTAMP, new TimestampArrayCodec()) };

	private CodecConfig() {
		_loadClassCodec();
		_loadClassCodecType();
		_loadCodecTypeCodec();
		_loadCodecTypeClass();
	}

	private static CodecConfig instance;

	public static CodecConfig load() {
		if (instance == null) {
			synchronized (CodecConfig.class) {
				if (instance == null) {
					instance = new CodecConfig();
				}
			}
		}

		return instance;
	}

	/**
	 * 根据对象查找相关的Codec.
	 * 
	 * @param obj
	 *            被序列化的对象.
	 * @return 相关的Codec
	 * @throws CodecException
	 *             此对象不能被序列化
	 */
	public Codec lookup(Object obj) throws CodecException {
		if (obj == null) {
			return classCodecPool.get(Object.class);
		}

		// 判断是否是异常类型.
		if (obj instanceof Throwable) {
			return classCodecPool.get(Throwable.class);
		}

		Class<?> clazz = obj.getClass();
		// 判断是否是基本类型, 如果是就从基本类型中找到Codec
		Codec codec = classCodecPool.get(clazz);
		if (codec != null)
			return codec;

		// 如果是枚举则使用枚举的序列化
		if (clazz.isEnum()) {
			return classCodecPool.get(Enum.class);
		}

		// 如果是数组则判断是对象的数组还是枚举的数组
		if (clazz.isArray()) {
			if (clazz.getComponentType().isEnum()) {
				return classCodecPool.get(Enum[].class);
			} else {
				return classCodecPool.get(Object[].class);
			}
		}

		_checkCodecable(clazz);
		return classCodecPool.get(Object.class);
	}

	/**
	 * 根据序列化类型查找相关的Codec.
	 * 
	 * @param type
	 *            序列化类型
	 * @return 相关的Codec
	 * @throws CodecException
	 *             没有找到相关的Codec
	 */
	public Codec lookup(byte type) throws CodecException {
		Codec codec = codecTypeCodecPool.get(type);

		if (codec != null) {
			return codec;
		}

		throw new CodecException("找不到相关的codec(type:" + type + ")");
	}

	/**
	 * 获取CodecType.
	 * 
	 * @param obj
	 *            根据这个对象获取类型.
	 * @return 编码类型
	 * @throws 获取编码类型失败
	 */
	public byte getCodecType(Object obj) throws CodecException {
		if (obj == null) {
			return classCodecTypePool.get(Object.class);
		}

		Class<?> clazz = obj.getClass();
		if (clazz.isEnum()) {
			return classCodecTypePool.get(Enum.class);
		}

		Byte type = classCodecTypePool.get(clazz);
		if (type != null) {
			return type;
		}

		if (clazz.isArray()) {
			if (clazz.getComponentType().isEnum()) {
				return classCodecTypePool.get(Enum[].class);
			} else {
				return classCodecTypePool.get(Object[].class);
			}
		}

		_checkCodecable(clazz);
		return classCodecTypePool.get(Object.class);
	}

	/**
	 * 根据序列化类型查找被序列化对象的class
	 * 
	 * @param type
	 *            序列化类型
	 * @return 被序列化对象的class
	 * @throws CodecException
	 *             没有找到class
	 */
	public Class<?> getClassByType(byte type) throws CodecException {
		Class<?> clazz = null;
		clazz = codecTypeClassPool.get(type);
		if (clazz != null) {
			return clazz;
		}

		throw new CodecException("不能识别的类型(type:" + type + ")");
	}

	/**
	 * 加载Class : Codec的映射关系
	 */
	private void _loadClassCodec() {
		for (ConfigItem item : config) {
			classCodecPool.put(item.clazz, item.codec);
		}
	}

	/**
	 * 加载Class : CodecType的映射关系
	 */
	private void _loadClassCodecType() {
		for (ConfigItem item : config) {
			classCodecTypePool.put(item.clazz, item.ct);
		}
	}

	/**
	 * 加载CodecType : Codec的映射关系
	 */
	private void _loadCodecTypeCodec() {
		for (ConfigItem item : config) {
			if (item.codec != null) {
				codecTypeCodecPool.put(item.ct, item.codec);
//				if (log.isDebugEnabled()) {
//					log.debug("load " + item.ct + ":" + item.codec);
//				}
			}
		}
	}

	/**
	 * 加载CodecType : Class的映射关系.
	 */
	private void _loadCodecTypeClass() {
		for (ConfigItem item : config) {
			codecTypeClassPool.put(item.ct, item.clazz);
		}
	}

	/**
	 * 判断对象是否能够被序列化
	 */
	private void _checkCodecable(Class<?> c) throws CodecException {
		// 如果是枚举类型则是可以被序列化的
		if (c.isEnum()) {
			return;
		}

		if (c == Class.class) {
			return;
		}

		// 判断对象是否实现Codecable接口
		Class<?>[] interfaces = BeansUtil.getInterfaces(c);
		for (Class<?> interf : interfaces) {
			if (interf == Serializable.class) {
				return;
			}
		}

		throw new CodecException("被序列化的对象(" + c + ")是不被支持的序列化类型也没有实现Serializable接口，序列化失败");
	}

	/**
	 * 配置项.
	 */
	class ConfigItem {
		/**
		 * 被序列化类型.
		 */
		public Class<?> clazz;
		/**
		 * 编码标志位.
		 */
		public byte ct;
		/**
		 * 编码类.
		 */
		public Codec codec;

		public ConfigItem(Class<?> clazz, byte ct, Codec codec) {
			this.clazz = clazz;
			this.ct = ct;
			this.codec = codec;
		}
	}
}
