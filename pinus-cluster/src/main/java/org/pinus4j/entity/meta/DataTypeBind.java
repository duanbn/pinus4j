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

package org.pinus4j.entity.meta;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据库类型映射.
 * 
 * @author duanbn
 */
public enum DataTypeBind {

	/**
	 * boolean类型
	 */
	BOOL("char", 0),
	/**
	 * byte类型
	 */
	BYTE("tinyint", 0),
	/**
	 * char类型
	 */
	CHAR("char", "''"),
	/**
	 * short类型
	 */
	SHORT("smallint", 0),
	/**
	 * int 类型
	 */
	INT("int", 0),
	/**
	 * long类型
	 */
	LONG("bigint", 0),
	/**
	 * float类型
	 */
	FLOAT("float", 0.0),
	/**
	 * double类型
	 */
	DOUBLE("double", 0.0),
	/**
	 * String类型
	 */
	STRING("varchar", "''"),
	/**
	 * 文本类型
	 */
	TEXT("text", null),
	/**
	 * 日期类型
	 */
	DATETIME("datetime", "'0000-00-00 00:00:00'"),
	/**
	 * 时间类型
	 */
	UPDATETIME("timestamp", "'0000-00-00 00:00:00'");

	/**
	 * 数据库类型
	 */
	private String dbType;
	/**
	 * 类型的默认值
	 */
	private Object defaultValue;

    private static final Map<String, DataTypeBind> MAP = new HashMap<String, DataTypeBind>(13);

    static {
        for (DataTypeBind value : DataTypeBind.values()) {
            MAP.put(value.getDBType(), value);
        }
    }

	/**
	 * 构造方法
	 * 
	 * @param dbType
	 *            数据库类型
	 * @param defaultValue
	 *            默认值
	 */
	DataTypeBind(String dbType, Object defaultValue) {
		this.dbType = dbType;
		this.defaultValue = defaultValue;
	}

	public String getDBType() {
		return this.dbType;
	}

	public Object getDefaultValue() {
		return this.defaultValue;
	}

    /**
     * get enum object by enum value.
     */
	public static DataTypeBind getEnum(String dbType) {
        return MAP.get(dbType);
	}

	public static DataTypeBind getEnum(Class<?> fieldType) {
		if (fieldType == Boolean.TYPE || fieldType == Boolean.class) {
			return BOOL;
		} else if (fieldType == Byte.TYPE || fieldType == Byte.class) {
			return BYTE;
		} else if (fieldType == Character.TYPE || fieldType == Character.class) {
			return CHAR;
		} else if (fieldType == Short.TYPE || fieldType == Short.class) {
			return SHORT;
		} else if (fieldType == Integer.TYPE || fieldType == Integer.class) {
			return INT;
		} else if (fieldType == Long.TYPE || fieldType == Long.class) {
			return LONG;
		} else if (fieldType == Float.TYPE || fieldType == Float.class) {
			return FLOAT;
		} else if (fieldType == Double.TYPE || fieldType == Double.class) {
			return DOUBLE;
		} else if (fieldType == String.class) {
			return STRING;
		}

		throw new IllegalStateException("不支持的的类型" + fieldType);
	}
}
