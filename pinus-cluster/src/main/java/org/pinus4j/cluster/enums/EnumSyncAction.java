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
