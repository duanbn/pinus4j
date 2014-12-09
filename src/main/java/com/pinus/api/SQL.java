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
		String s = null;

		for (Object param : params) {
			if (param instanceof String)
				s = sql.replaceFirst("\\?", "'" + String.valueOf(param) + "'");
			else
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
