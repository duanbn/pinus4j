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

package org.pinus.cluster.enums;

import org.pinus.util.HashUtil;

/**
 * hash算法枚举.
 * 
 * @author duanbn
 * 
 */
public enum HashAlgoEnum {
	/**
	 * 加法hash
	 */
	ADDITIVE("additive"),
	/**
	 * 旋转hash
	 */
	ROTATING("rotating"),
	/**
	 * onebyone hash
	 */
	ONEBYONE("oneByOne"),
	/**
	 * bernstein hash
	 */
	BERNSTEIN("bernstein"),
	/**
	 * fnv hash
	 */
	FNV("fnv"),
	/**
	 * rs hash
	 */
	RS("rs"),
	/**
	 * js hash
	 */
	JS("js"),
	/**
	 * pjw hash
	 */
	PJW("pjw"),
	/**
	 * elf hash
	 */
	ELF("elf"),
	/**
	 * bkdr hash
	 */
	BKDR("bkdr"),
	/**
	 * sdbm hash
	 */
	SDBM("sdbm"),
	/**
	 * djb hash
	 */
	DJB("djb"),
	/**
	 * dek hash
	 */
	DEK("dek"),
	/**
	 * ap hash
	 */
	AP("ap"),
	/**
	 * java自带hash
	 */
	JAVA("java"),
	/**
	 * min hash
	 */
	MIX("mix");

	private String value;

	private HashAlgoEnum(String value) {
		this.value = value;
	}

	public long hash(String key) {
		long hashValue = -1;
		switch (this) {
		case ADDITIVE:
			hashValue = HashUtil.additiveHash(key);
			break;
		case AP:
			hashValue = HashUtil.APHash(key);
			break;
		case BERNSTEIN:
			hashValue = HashUtil.bernstein(key);
			break;
		case BKDR:
			hashValue = HashUtil.BKDRHash(key);
			break;
		case DEK:
			hashValue = HashUtil.DEKHash(key);
			break;
		case DJB:
			hashValue = HashUtil.DJBHash(key);
			break;
		case ELF:
			hashValue = HashUtil.ELFHash(key);
			break;
		case FNV:
			hashValue = HashUtil.FNVHash1(key);
			break;
		case JAVA:
			hashValue = HashUtil.java(key);
			break;
		case JS:
			hashValue = HashUtil.JSHash(key);
			break;
		case MIX:
			hashValue = HashUtil.mixHash(key);
			break;
		case ONEBYONE:
			hashValue = HashUtil.oneByOneHash(key);
			break;
		case PJW:
			hashValue = HashUtil.PJWHash(key);
			break;
		case ROTATING:
			hashValue = HashUtil.rotatingHash(key);
			break;
		case RS:
			hashValue = HashUtil.RSHash(key);
			break;
		case SDBM:
			hashValue = HashUtil.SDBMHash(key);
			break;
		}
		return Math.abs(hashValue);
	}

	public String getValue() {
		return value;
	}

	public static HashAlgoEnum getEnum(String value) {
		for (HashAlgoEnum type : HashAlgoEnum.values()) {
			if (type.value.equals(value)) {
				return type;
			}
		}
		throw new IllegalArgumentException("找不到相关枚举, value=" + value);
	}
}
