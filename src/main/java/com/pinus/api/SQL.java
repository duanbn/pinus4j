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

package com.pinus.api;

import java.util.List;

import com.pinus.datalayer.SQLParser;

/**
 * SQL查询. sql语句中的变量使用"?"表示.
 * 
 * @author duanbn
 */
public class SQL {

	/**
	 * sql语句
	 */
	private String sql;

	/**
	 * 查询参数
	 */
	private Object[] params;

	private SQL() {
	}

	private SQL(String sql, Object... params) {
		this.sql = sql;
		this.params = params;
	}

	public static final SQL valueOf(String sql, Object... params) {
		SQL obj = new SQL(sql, params);

		return obj;
	}

	public List<String> getTableNames() {
		return SQLParser.parseTableName(sql);
	}

	@Override
	public String toString() {
		String s = sql;

		if (params != null && params.length > 0) {
			for (Object param : params) {
				if (param instanceof String)
					s = s.replaceFirst("\\?", "'" + String.valueOf(param) + "'");
				else
					s = s.replaceFirst("\\?", String.valueOf(param));
			}
		} else {
			s = sql;
		}

		return s;
	}

	public String getSql() {
		return sql;
	}

	public void setParams(Object[] params) {
		this.params = params;
	}

	public Object[] getParams() {
		return params;
	}

}
