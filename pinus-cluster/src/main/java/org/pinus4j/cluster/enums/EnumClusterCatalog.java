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

package org.pinus4j.cluster.enums;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public enum EnumClusterCatalog {

	/**
	 * mysql.
	 */
	MYSQL("mysql"),
	/**
	 * redis.
	 */
	REDIS("redis");

    private static final Map<String, EnumClusterCatalog> map;

    static {
        map = new HashMap<String, EnumClusterCatalog>(2);
        for (EnumClusterCatalog type : EnumClusterCatalog.values()) {
            map.put(type.value, type);
		}
    }

	private String value;

	private EnumClusterCatalog(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}

	public static EnumClusterCatalog getEnum(String value) {
        EnumClusterCatalog catalog = map.get(value);

        if (catalog == null) 
            throw new IllegalArgumentException("找不到相关枚举, value=" + value);
        
        return catalog;
	}
}
