package com.pinus.api.query;

import java.util.ArrayList;
import java.util.List;

import com.pinus.util.StringUtils;

/**
 * 查询对象实现.
 * 
 * @author duanbn
 */
public class QueryImpl implements IQuery, Cloneable {

	/**
	 * 保存取值的字段.
	 */
	private List<String> fieldList = new ArrayList<String>();

	/**
	 * 保存查询条件.
	 */
	private List<Condition> condList = new ArrayList<Condition>();

	/**
	 * 保存排序条件
	 */
	private List<OrderBy> orderList = new ArrayList<OrderBy>();

	/**
	 * 分页开始偏移量
	 */
	private int start = -1;
	/**
	 * 分页大小
	 */
	private int limit = -1;

    @Override
    public IQuery clone() {
        QueryImpl clone = new QueryImpl();
        clone.setFieldList(new ArrayList(this.fieldList));
        clone.setCondList(new ArrayList(this.condList));
        clone.setOrderList(new ArrayList(this.orderList));
        clone.setStart(this.start);
        clone.setLimit(this.limit);
        return clone;
    }

	// @Override
	// public IQuery addField(String... fields) {
	// if (fields != null && fields.length > 0) {
	// for (String field : fields) {
	// this.fieldList.add(field);
	// }
	// }
	// return this;
	// }

	// @Override
	// public String getField() {
	// if (this.fieldList.isEmpty()) {
	// return "*";
	// }
	//
	// StringBuilder fieldSql = new StringBuilder();
	// for (String field : fieldList) {
	// fieldSql.append(field).append(",");
	// }
	// fieldSql.deleteCharAt(fieldSql.length() - 1);
	// return fieldSql.toString();
	// }

	@Override
	public String getWhereSql() {
		StringBuilder SQL = new StringBuilder();
		// 添加查询条件
		if (!condList.isEmpty()) {
            SQL.append(" WHERE ");
			for (Condition cond : condList) {
				SQL.append(cond.getSql()).append(" AND ");
			}
			SQL.delete(SQL.lastIndexOf(" AND "), SQL.length());
		}
		// 添加排序条件
		if (!orderList.isEmpty()) {
			SQL.append(" ORDER BY ");
			for (OrderBy orderBy : orderList) {
				SQL.append(orderBy.getField());
				SQL.append(" ");
				SQL.append(orderBy.getOrder().getValue());
				SQL.append(",");
			}
			SQL.deleteCharAt(SQL.length() - 1);
		}
		// 添加分页
		if (start != -1 && limit != -1) {
			SQL.append(" LIMIT ").append(start).append(",").append(limit);
		} else if (limit != -1) {
			SQL.append(" LIMIT ").append(limit);
		}
		return SQL.toString();
	}

	@Override
	public IQuery add(Condition cond) {
		if (cond == null) {
			throw new IllegalArgumentException("参数错误, cond=null");
		}

		condList.add(cond);
		return this;
	}

	@Override
	public IQuery orderBy(String field, Order order) {
		if (StringUtils.isBlank(field)) {
			throw new IllegalArgumentException("参数错误, field=" + field);
		}
		if (order == null) {
			throw new IllegalArgumentException("参数错误, order=null");
		}

		orderList.add(new OrderBy(field, order));
		return this;
	}

	@Override
	public IQuery limit(int start, int limit) {
		if (start < 0 || limit <= 0) {
			throw new IllegalArgumentException("分页参数错误, start" + start + ", limit=" + limit);
		}

		this.start = start;
		this.limit = limit;

		return this;
	}

	@Override
	public IQuery limit(int limit) {
		if (limit <= 0) {
			throw new IllegalArgumentException("设置limit参数错误， limit=" + limit);
		}

		this.limit = limit;

		return this;
	}

	@Override
	public String toString() {
		StringBuilder info = new StringBuilder();
		if (!fieldList.isEmpty()) {
			info.append("fields:");
			for (String field : fieldList) {
				info.append(field).append(",");
			}
			info.deleteCharAt(info.length() - 1);
		}
		if (StringUtils.isNotBlank(getWhereSql())) {
			info.append("wheresql:").append(getWhereSql());
		}
		return info.toString();
	}

    public void setFieldList(List<String> fieldList) {
        this.fieldList = fieldList;
    }

    public void setCondList(List<Condition> condList) {
        this.condList = condList;
    }

    public void setOrderList(List<OrderBy> orderList) {
        this.orderList = orderList;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

	/**
	 * 排序条件.
	 */
	private class OrderBy {
		private String field;
		private Order order;

		public OrderBy(String field, Order order) {
			this.field = field;
			this.order = order;
		}

		public String getField() {
			return field;
		}

		public Order getOrder() {
			return order;
		}

	}

}
