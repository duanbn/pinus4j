package com.pinus.api.enums;

/**
 * 实体和数据表同步的动作.
 * 
 * @author duanbn
 *
 */
public enum EnumSyncAction {

	/**
	 * 不同步.
	 */
	NONE,
	/**
	 * 只创建表，如果表已经存在则忽略
	 */
	CREATE,
	/**
	 * 更新表. 只做列类型变更和添加列，不会做任何删除操作.
	 */
	UPDATE;

}
