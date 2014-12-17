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

package org.pinus.util;

/**
 * Hash算法工具.
 * 
 * @author duanbn
 */
public class HashUtil {

	/**
	 * 加法Hash算法.
	 * 
	 * @return hash值
	 */
	public static int additiveHash(String key) {
		int hash, i;
		for (hash = key.length(), i = 0; i < key.length(); i++)
			hash += key.charAt(i);
		return (hash % 983);
	}

	/**
	 * 旋转Hash
	 * 
	 * @return hash值
	 */
	public static int rotatingHash(String key) {
		int hash, i;
		for (hash = key.length(), i = 0; i < key.length(); ++i)
			hash = (hash << 4) ^ (hash >> 28) ^ key.charAt(i);
		return (hash ^ (hash >> 10) ^ (hash >> 20));
	}

	static int M_MASK = 0x8765fed1;

	/**
	 * one by one Hash
	 * 
	 * @return hash值
	 */
	public static int oneByOneHash(String key) {
		int hash, i;
		for (hash = 0, i = 0; i < key.length(); ++i) {
			hash += key.charAt(i);
			hash += (hash << 10);
			hash ^= (hash >> 6);
		}
		hash += (hash << 3);
		hash ^= (hash >> 11);
		hash += (hash << 15);
		return (hash & M_MASK);
	}

	/**
	 * bernstein hash
	 * 
	 * @return hash值
	 */
	public static int bernstein(String key) {
		int hash = 0;
		for (int i = 0; i < key.length(); ++i)
			hash = 33 * hash + key.charAt(i);
		return hash;
	}

	static int M_SHIFT = 0;

	/**
	 * 32位的FNV算法
	 * 
	 * @return hash值
	 */
	public static int FNVHash(byte[] data) {
		int hash = (int) 2166136261L;
		for (byte b : data)
			hash = (hash * 16777619) ^ b;
		if (M_SHIFT == 0)
			return hash;
		return (hash ^ (hash >> M_SHIFT)) & M_MASK;
	}

	/**
	 * 改进的32位FNV算法1
	 * 
	 * @return hash值
	 */
	public static int FNVHash1(byte[] data) {
		final int p = 16777619;
		int hash = (int) 2166136261L;
		for (byte b : data)
			hash = (hash ^ b) * p;
		hash += hash << 13;
		hash ^= hash >> 7;
		hash += hash << 3;
		hash ^= hash >> 17;
		hash += hash << 5;
		return hash;
	}

	/**
	 * 改进的32位FNV算法1
	 * 
	 * @return hash值
	 */
	public static int FNVHash1(String data) {
		final int p = 16777619;
		int hash = (int) 2166136261L;
		for (int i = 0; i < data.length(); i++)
			hash = (hash ^ data.charAt(i)) * p;
		hash += hash << 13;
		hash ^= hash >> 7;
		hash += hash << 3;
		hash ^= hash >> 17;
		hash += hash << 5;
		return hash;
	}

	/**
	 * int hash
	 * 
	 * @return hash值
	 */
	public static int intHash(int key) {
		key += ~(key << 15);
		key ^= (key >>> 10);
		key += (key << 3);
		key ^= (key >>> 6);
		key += ~(key << 11);
		key ^= (key >>> 16);
		return key;
	}

	/**
	 * RS算法hash
	 * 
	 * @return hash值
	 */
	public static int RSHash(String str) {
		int b = 378551;
		int a = 63689;
		int hash = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = hash * a + str.charAt(i);
			a = a * b;
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * JS hash算法o
	 * 
	 * @return hash值
	 */
	public static int JSHash(String str) {
		int hash = 1315423911;

		for (int i = 0; i < str.length(); i++) {
			hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * PJW算法
	 * 
	 * @return hash值
	 */
	public static int PJWHash(String str) {
		int BitsInUnsignedInt = 32;
		int ThreeQuarters = (BitsInUnsignedInt * 3) / 4;
		int OneEighth = BitsInUnsignedInt / 8;
		int HighBits = 0xFFFFFFFF << (BitsInUnsignedInt - OneEighth);
		int hash = 0;
		int test = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = (hash << OneEighth) + str.charAt(i);

			if ((test = hash & HighBits) != 0) {
				hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
			}
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * ELF算法.
	 * 
	 * @return hash值
	 */
	public static int ELFHash(String str) {
		int hash = 0;
		int x = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = (hash << 4) + str.charAt(i);
			if ((x = (int) (hash & 0xF0000000L)) != 0) {
				hash ^= (x >> 24);
				hash &= ~x;
			}
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * BKDR hash算法.
	 * 
	 * @return hash值
	 */
	public static int BKDRHash(String str) {
		int seed = 131; // 31 131 1313 13131 131313 etc..
		int hash = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = (hash * seed) + str.charAt(i);
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * SDBM hash算法.
	 * 
	 * @return hash值
	 */
	public static int SDBMHash(String str) {
		int hash = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * DJB hash算法.
	 * 
	 * @return hash值.
	 */
	public static int DJBHash(String str) {
		int hash = 5381;

		for (int i = 0; i < str.length(); i++) {
			hash = ((hash << 5) + hash) + str.charAt(i);
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * DEK hash算法.
	 * 
	 * @return hash值
	 */
	public static int DEKHash(String str) {
		int hash = str.length();

		for (int i = 0; i < str.length(); i++) {
			hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
		}

		return (hash & 0x7FFFFFFF);
	}

	/**
	 * AP hash算法.
	 * 
	 * @return hash值
	 */
	public static int APHash(String str) {
		int hash = 0;

		for (int i = 0; i < str.length(); i++) {
			hash ^= ((i & 1) == 0) ? ((hash << 7) ^ str.charAt(i) ^ (hash >> 3))
					: (~((hash << 11) ^ str.charAt(i) ^ (hash >> 5)));
		}

		// return (hash & 0x7FFFFFFF);
		return hash;
	}

	/**
	 * Java自带hash算法.
	 * 
	 * @return hash值
	 */
	public static int java(String str) {
		int h = 0;
		int off = 0;
		int len = str.length();
		for (int i = 0; i < len; i++) {
			h = 31 * h + str.charAt(off++);
		}
		return h;
	}

	/**
	 * 混合hash算法，输出64位的值
	 * 
	 * @return hash值
	 */
	public static long mixHash(String str) {
		long hash = str.hashCode();
		hash <<= 32;
		hash |= FNVHash1(str);
		return hash;
	}
}
