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

/**
 * 字符串工具类.
 * 
 * @author duanbn
 */
public class StringUtils {

    /**
     * 将首字母变成大写.
     * 
     * @param value
     * @return
     */
    public static String upperFirstLetter(String value) {
        if (value == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        return value.substring(0, 1).toUpperCase() + value.substring(1);
    }

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
        if (value == null || value.trim().equals("")) {
            return true;
        } else {
            return false;
        }
    }

}
