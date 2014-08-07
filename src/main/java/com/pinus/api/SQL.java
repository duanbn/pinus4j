package com.pinus.api;

/**
 * SQL查询. sql语句中的变量使用"?"表示.
 * 
 * @author duanbn
 */
public class SQL<T> {

	/**
	 * 数据对象class
	 */
	private Class<T> clazz;

	/**
	 * sql语句
	 */
	private String sql;

	/**
	 * 查询参数
	 */
	private Object[] params;

	public SQL(Class<T> clazz, String sql) {
		this(clazz, sql, null);
	}

	public SQL(Class<T> clazz, String sql, Object... params) {
		this.clazz = clazz;
		if (sql != null)
			this.sql = sql.toLowerCase();
		this.params = params;
	}

	@Override
	public String toString() {
		String s = null;

		for (Object param : params) {
			s = sql.replaceFirst("\\?", String.valueOf(param));
		}

		return s;
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
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
