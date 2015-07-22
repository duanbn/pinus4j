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

package org.pinus4j.api.query;

import java.util.ArrayList;
import java.util.List;

import org.pinus4j.utils.StringUtils;

/**
 * 查询对象实现.
 * 
 * @author duanbn
 */
public class QueryImpl implements IQuery, Cloneable {

	/**
	 * 保存取值的字段.
	 */
	private String[] fields;

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
	public int getStart() {
		return this.start;
	}

	@Override
	public int getLimit() {
		return this.limit;
	}

	@Override
	public boolean hasQueryFields() {
		return this.fields != null && this.fields.length > 0;
	}

	@Override
	public IQuery clone() {
		QueryImpl clone = new QueryImpl();
		clone.setFields(this.fields);
		clone.setCondList(new ArrayList<Condition>(this.condList));
		clone.setOrderList(new ArrayList<OrderBy>(this.orderList));
		clone.setStart(this.start);
		clone.setLimit(this.limit);
		return clone;
	}

	@Override
	public IQuery setFields(String... fields) {
		if (fields != null && fields.length > 0) {
			this.fields = fields;
		}
		return this;
	}

	@Override
	public String[] getFields() {
		return this.fields;
	}

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
		if (start > -1 && limit > -1) {
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
		if (fields != null && fields.length > 0) {
			info.append("fields:");
			for (String field : fields) {
				info.append(field).append(",");
			}
			info.deleteCharAt(info.length() - 1);
		}
		if (StringUtils.isNotBlank(getWhereSql())) {
			info.append(" wheresql:").append(getWhereSql());
		}
		return info.toString();
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
