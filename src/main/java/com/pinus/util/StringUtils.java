package com.pinus.util;

/**
 * 字符串工具类.
 * 
 * @author duanbn
 * 
 */
public class StringUtils {

	/**
	 * 去除字符串中的空格
	 * 
	 * @param value
	 * @return 去掉空格后的字符串
	 */
	public static String removeBlank(String value) {
		return value.replaceAll(" ", "");
	}

	/**
	 * 判断字符串不为空
	 * 
	 * @return
	 */
	public static boolean isNotBlank(String value) {
		return !isBlank(value);
	}

	/**
	 * 判断字符串是否空.
	 * 
	 * @param value
	 * @return true:是, false:否
	 */
	public static boolean isBlank(String value) {
		if (value == null || value.equals("")) {
			return true;
		} else {
			return false;
		}
	}

}
