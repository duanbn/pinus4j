package com.pinus.cluster.enums;

public enum EnumClusterCatalog {

	/**
	 * mysql.
	 */
	MYSQL("MYSQL"),
	/**
	 * redis.
	 */
	REDIS("REDIS");

	private String value;

	private EnumClusterCatalog(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}

	public static EnumClusterCatalog getEnum(String value) {
		for (EnumClusterCatalog type : EnumClusterCatalog.values()) {
			if (type.value.equals(value)) {
				return type;
			}
		}
		throw new IllegalArgumentException("找不到相关枚举, value=" + value);
	}
}
