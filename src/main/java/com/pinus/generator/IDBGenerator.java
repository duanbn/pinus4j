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

package com.pinus.generator;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

import com.pinus.api.enums.EnumSyncAction;
import com.pinus.cluster.beans.DBTable;
import com.pinus.exception.DDLException;

/**
 * 数据库表生成器接口.
 * 
 * @author duanbn
 */
public interface IDBGenerator {

	/**
	 * 扫描@Table类
	 * 
	 * @param scanPackage
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public List<DBTable> scanEntity(String scanPackage) throws IOException, ClassNotFoundException;

	/**
	 * 同步数据库表
	 * 
	 * @param table
	 *
	 * @throws DDLException
	 */
	public void syncTable(Connection conn, DBTable table) throws DDLException;

	/**
	 * 批量同步数据库表. 表下标从0开始.
	 *
	 * @param conn
	 *            数据库连接.
	 * @param table
	 *            库表
	 * @param num
	 *            需要生成的分表数
	 */
	public void syncTable(Connection conn, DBTable table, int num) throws DDLException;

	public void setSyncAction(EnumSyncAction syncAction);

}
