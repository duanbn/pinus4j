package com.pinus.api.enums;

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
