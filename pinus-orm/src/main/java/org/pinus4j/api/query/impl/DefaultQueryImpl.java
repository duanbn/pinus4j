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

package org.pinus4j.api.query.impl;

import java.util.ArrayList;
import java.util.List;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;

/**
 * 查询对象实现.
 * 
 * @author duanbn
 */
public class DefaultQueryImpl implements IQuery, Cloneable {

    /**
     * 保存取值的字段.
     */
    protected String[]        fields;

    /**
     * 保存查询条件.
     */
    protected List<Condition> condList  = new ArrayList<Condition>();

    /**
     * 保存排序条件
     */
    protected List<OrderBy>   orderList = new ArrayList<OrderBy>();

    /**
     * 分页开始偏移量
     */
    protected int             start     = -1;
    /**
     * 分页大小
     */
    protected int             limit     = -1;

    @Override
    public <T> T load() {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public <T> List<T> list() {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public Number count() {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery setMasterSlave(EnumDBMasterSlave masterSlave) {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery setUseCache(boolean useCache) {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery setFields(String... fields) {
        if (fields != null && fields.length > 0) {
            this.fields = fields;
        }
        return this;
    }

    @Override
    public IQuery setFields(Class<?> clazz, String... fields) {
        if (fields != null && fields.length > 0) {
            for (String field : fields) {
                field = BeansUtil.getFieldName(BeansUtil.getField(clazz, field));
            }
            this.fields = fields;
        }
        return this;
    }

    @Deprecated
    @Override
    public IQuery add(Condition cond) {
        return and(cond);
    }

    @Override
    public IQuery and(Condition cond) {
        if (cond == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        if (!condList.isEmpty())
            cond.setConditionRelation(ConditionRelation.AND);

        condList.add(cond);

        return this;
    }

    @Override
    public IQuery or(Condition cond) {
        if (cond == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        if (!condList.isEmpty())
            cond.setConditionRelation(ConditionRelation.OR);

        condList.add(cond);

        return this;
    }

    @Override
    public IQuery orderBy(String field, Order order, Class<?> clazz) {
        if (StringUtil.isBlank(field)) {
            throw new IllegalArgumentException("参数错误, field=" + field);
        }
        if (order == null) {
            throw new IllegalArgumentException("参数错误, order=null");
        }

        orderList.add(new OrderBy(field, order, clazz));
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
    public void clean() {
        this.fields = null;
        this.condList.clear();
        this.orderList.clear();
        this.start = -1;
        this.limit = -1;

    }

    public int getStart() {
        return this.start;
    }

    public int getLimit() {
        return this.limit;
    }

    public boolean hasQueryFields() {
        return this.fields != null && this.fields.length > 0;
    }

    public IQuery clone() {
        DefaultQueryImpl clone = new DefaultQueryImpl();
        clone.setFields(this.fields);
        clone.setCondList(new ArrayList<Condition>(this.condList));
        clone.setOrderList(new ArrayList<OrderBy>(this.orderList));
        clone.setStart(this.start);
        clone.setLimit(this.limit);
        return clone;
    }

    public String[] getFields() {
        return this.fields;
    }

    public String getWhereSql() {
        StringBuilder SQL = new StringBuilder();
        // 添加查询条件
        if (!condList.isEmpty()) {
            SQL.append(" where ");

            Condition cond = null;
            for (int i = 0; i < condList.size(); i++) {
                cond = condList.get(i);
                if (i > 0) {
                    SQL.append(" ").append(cond.getConditionRelation().getValue()).append(" ").append(cond.getSql());
                } else {
                    SQL.append(cond.getSql()); // first one
                }
            }
        }
        // 添加排序条件
        if (!orderList.isEmpty()) {
            SQL.append(" order by ");
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
            SQL.append(" limit ").append(start).append(",").append(limit);
        } else if (limit != -1) {
            SQL.append(" limit ").append(limit);
        }
        return SQL.toString();
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
        if (StringUtil.isNotBlank(getWhereSql())) {
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
        private Order  order;

        public OrderBy(String field, Order order, Class<?> clazz) {
            this.field = BeansUtil.getFieldName(BeansUtil.getField(clazz, field));
            this.order = order;
        }

        public String getField() {
            return field;
        }

        public Order getOrder() {
            return order;
        }

    }

    /**
     * 只给Condition使用.
     * 
     * @author shanwei Jul 29, 2015 7:06:33 PM
     */
    enum ConditionRelation {
        AND("and"),
        OR("or");

        private String value;

        private ConditionRelation(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

}
