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

package org.pinus4j.api.enums;

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
