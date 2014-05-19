package com.pinus.api.query;

import com.pinus.util.StringUtils;

/**
 * 查询条件.
 * 
 * @author duanbn
 */
public class Condition {

	/**
	 * 条件字段.
	 */
	private String field;
	/**
	 * 条件值.
	 */
	private Object[] values;
	/**
	 * 条件枚举.
	 */
	private QueryOpt opt;

	/**
	 * 保存or查询.
	 */
	private Condition[] orCond;

	/**
	 * 构造方法. 防止调用者直接创建此对象.
	 */
	private Condition() {
	}

	private Condition(Condition... conds) {
		this.orCond = conds;
	}

	/**
	 * 构造方法.
	 * 
	 * @param field
	 *            条件字段
	 * @param values
	 *            条件值
	 * @param opt
	 *            条件枚举
	 */
	private Condition(String field, Object[] values, QueryOpt opt) {
		if (StringUtils.isBlank(field)) {
			throw new IllegalArgumentException("条件字段不能为空, condition field=" + field);
		}
		if (values == null || values.length == 0) {
			throw new IllegalArgumentException("参数错误, condition value=" + values);
		}

		this.field = field;
		this.values = values;
		this.opt = opt;
	}

	/**
	 * 返回当前条件对象表示的sql语句.
	 * 
	 * @return sql语句
	 */
	public String getSql() {
		StringBuilder SQL = new StringBuilder();
		if (orCond != null && orCond.length > 0) {
			SQL.append("(");
			for (Condition cond : orCond) {
				SQL.append(cond.getSql()).append(" OR ");
			}
			SQL.delete(SQL.lastIndexOf(" OR "), SQL.length());
			SQL.append(")");
			return SQL.toString();
		} else {
			SQL.append(field).append(" ").append(opt.getSymbol()).append(" ");
			switch (opt) {
			case IN:
				SQL.append("(");
				for (Object value : values) {
					if (value instanceof String) {
						SQL.append("'").append(value).append("'");
					} else if (value instanceof Boolean) {
						if ((Boolean) value) {
							SQL.append("'").append("1").append("'");
						} else {
							SQL.append("'").append("0").append("'");
						}
					} else {
						SQL.append(value);
					}
					SQL.append(",");
				}
				SQL.deleteCharAt(SQL.length() - 1);
				SQL.append(")");
				break;
			default:
				Object value = values[0];
				if (value instanceof String) {
					SQL.append("'").append(value).append("'");
				} else if (value instanceof Boolean) {
					if ((Boolean) value) {
						SQL.append("'").append("1").append("'");
					} else {
						SQL.append("'").append("0").append("'");
					}
				} else {
					SQL.append(value);
				}
				break;
			}
			return SQL.toString();
		}
	}

	@Override
	public String toString() {
		return getSql();
	}

	/**
	 * 等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition eq(String field, Object value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.EQ);
		return cond;
	}

	/**
	 * 不等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition noteq(String field, Object value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.NOTEQ);
		return cond;
	}

	/**
	 * 大于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition gt(String field, Object value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.GT);
		return cond;
	}

	/**
	 * 大于等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition gte(String field, Object value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.GTE);
		return cond;
	}

	/**
	 * 小于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition lt(String field, Object value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.LT);
		return cond;
	}

	/**
	 * 小于等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition lte(String field, Object value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.LTE);
		return cond;
	}

	/**
	 * in操作.
	 * 
	 * @param field
	 *            条件字段
	 * @param values
	 *            字段值
	 * 
	 * @return 当前条件对象
	 */
	public static Condition in(String field, Object... values) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN);
		return cond;
	}

	/**
	 * like查询.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition like(String field, String value) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, new Object[] { value }, QueryOpt.LIKE);
		return cond;
	}

	/**
	 * 或查询.
	 * 
	 * @param conds
	 *            查询条件
	 */
	public static Condition or(Condition... conds) {
		if (conds == null || conds.length < 2) {
			throw new IllegalArgumentException("参数错误, or查询条件最少为2个");
		}
		Condition cond = new Condition(conds);
		return cond;
	}

}
