package com.pinus.api.enums;

/**
 * 考拉存储中间件支持的数据库类型.
 * 
 * @author duanbn
 */
public enum EnumMode {

	/**
	 * 单机模式.
	 */
	STANDALONE("standalone"),
	/**
	 * 分布式模式. 需要使用zookeeper做分布式协调
	 */
	DISTRIBUTED("distributed");

	/**
	 * 数据库驱动.
	 */
	private String mode;

	private EnumMode(String mode) {
		this.mode = mode;
	}

	/**
	 * 获取驱动.
	 * 
	 * @return 驱动
	 */
	public String getMode() {
		return this.mode;
	}

}
