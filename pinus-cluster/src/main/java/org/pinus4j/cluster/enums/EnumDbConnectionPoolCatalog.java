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

/**
 * 数据库连接池配置类型.
 * 
 * @author duanbn
 *
 */
public enum EnumDbConnectionPoolCatalog {

	/**
	 * env
	 */
	ENV("env"),
	/**
	 * app
	 */
	APP("app");

	private static final Map<String, EnumDbConnectionPoolCatalog> map = new HashMap<String, EnumDbConnectionPoolCatalog>();

	static {
		for (EnumDbConnectionPoolCatalog catalog : EnumDbConnectionPoolCatalog.values()) {
			map.put(catalog.getValue(), catalog);
		}
	}

	private String value;

	private EnumDbConnectionPoolCatalog(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public static EnumDbConnectionPoolCatalog getEnum(String value) {
		return map.get(value);
	}

}
