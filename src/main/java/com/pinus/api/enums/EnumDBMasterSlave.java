package com.pinus.api.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据库主从选择枚举. 每一个数据库集群可以有若干个从库，从库和主库的数据是实时同步的. 目前先支持4个从库，如果有更多的从库可以添加此枚举的值即可.
 *
 * @author duanbn
 */
public enum EnumDBMasterSlave {

	/**
	 * 主库
	 */
	MASTER(0),
	/**
	 * 第一个从库.
	 */
	SLAVE0(0),
	/**
	 * 第二个从库.
	 */
	SLAVE1(1),
	/**
	 * 第三个从库.
	 */
	SLAVE2(2),
	/**
	 * 第四个从库.
	 */
	SLAVE3(3);

	private static final Map<Integer, EnumDBMasterSlave> masterMap = new HashMap<Integer, EnumDBMasterSlave>();
	private static final Map<Integer, EnumDBMasterSlave> slaveMap = new HashMap<Integer, EnumDBMasterSlave>();

	static {
		masterMap.put(0, MASTER);

		slaveMap.put(0, SLAVE0);
		slaveMap.put(1, SLAVE1);
		slaveMap.put(2, SLAVE2);
		slaveMap.put(3, SLAVE3);
	}

	private int value;

	private EnumDBMasterSlave(int value) {
		this.value = value;
	}

	public int getValue() {
		return this.value;
	}

	public static EnumDBMasterSlave getMasterEnum(int value) {
		return masterMap.get(value);
	}

	public static EnumDBMasterSlave getSlaveEnum(int value) {
		return slaveMap.get(value);
	}

}
