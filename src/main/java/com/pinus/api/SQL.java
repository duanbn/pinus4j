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

	public SQL(String sql, Object... params) {
		if (sql != null)
			this.sql = sql.toLowerCase();
		this.params = params;
	}

	public List<String> getTableNames() {
		return SQLParser.parseTableName(sql);
	}

	@Override
	public String toString() {
		String s = null;

		for (Object param : params) {
			s = sql.replaceFirst("\\?", String.valueOf(param));
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
