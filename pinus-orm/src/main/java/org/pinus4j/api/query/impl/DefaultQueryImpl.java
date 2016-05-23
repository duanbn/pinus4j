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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javax.management.Query;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;

import com.google.common.collect.Lists;

/**
 * 查询对象实现.
 * 
 * @author duanbn
 */
public class DefaultQueryImpl<T> implements IQuery<T>, Cloneable {

    protected Class<T>        clazz;

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
    public T load() {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public List<T> list() {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public Number count() {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery<T> setShardingKey(IShardingKey<?> shardingKey) {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery<T> setMasterSlave(EnumDBMasterSlave masterSlave) {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery<T> setUseCache(boolean useCache) {
        throw new UnsupportedOperationException("not support");
    }

    @Override
    public IQuery<T> setFields(String... fields) {
        if (fields != null && fields.length > 0) {
            this.fields = fields;
        }
        return this;
    }

    @Override
    public IQuery<T> setFields(Class<?> clazz, String... fields) {
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
    public IQuery<T> add(Condition cond) {
        return and(cond);
    }

    @Override
    public IQuery<T> and(Condition cond) {
        if (cond == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        if (!condList.isEmpty())
            cond.setConditionRelation(ConditionRelation.AND);

        condList.add(cond);

        return this;
    }

    @Override
    public IQuery<T> or(Condition cond) {
        if (cond == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        if (!condList.isEmpty())
            cond.setConditionRelation(ConditionRelation.OR);

        condList.add(cond);

        return this;
    }

    @Override
    public IQuery<T> orderBy(String field, Order order) {
        return orderBy(field, order, clazz);
    }

    @Override
    public IQuery<T> orderBy(String field, Order order, Class<?> clazz) {
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
    public IQuery<T> limit(int start, int limit) {
        if (start < 0 || limit <= 0) {
            throw new IllegalArgumentException("分页参数错误, start" + start + ", limit=" + limit);
        }

        this.start = start;
        this.limit = limit;

        return this;
    }

    @Override
    public IQuery<T> limit(int limit) {
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

    public String[] getFields() {
        return this.fields;
    }

    public List<Condition> getCondList() {
        return this.condList;
    }

    public List<OrderBy> getOrderList() {
        return this.orderList;
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

    public boolean isEffect() {
        if (this.condList.isEmpty() && this.orderList.isEmpty() && start == -1 && limit == -1) {
            return false;
        }

        return true;
    }

    public IQuery<T> clone() {
        DefaultQueryImpl<T> clone = new DefaultQueryImpl<T>();
        clone.fields = this.fields;
        clone.condList.addAll(this.condList);
        clone.orderList.addAll(this.orderList);
        clone.start = this.start;
        clone.limit = this.limit;
        return clone;
    }

    public SQL getWhereSql() {
        StringBuilder sqlText = new StringBuilder();
        StringBuilder whereSql = new StringBuilder();
        StringBuilder orderSql = new StringBuilder();
        StringBuilder findInSet = new StringBuilder();
        List<Object> paramList = Lists.newArrayList();

        // 添加查询条件
        // 当查询条件中只有一个in查询，则需要对此in查询进行排序
        List<Condition> inConditionList = Lists.newArrayList();
        if (!condList.isEmpty()) {
            whereSql.append(" where ");

            Condition cond = null;
            for (int i = 0; i < condList.size(); i++) {
                cond = condList.get(i);
                if (i > 0) {
                    whereSql.append(" ").append(cond.getConditionRelation().getValue()).append(" ")
                            .append(cond.getSql().getSql());
                } else {
                    whereSql.append(cond.getSql().getSql()); // first one
                }

                if (QueryOpt.IN == cond.getOpt()) {
                    inConditionList.add(cond);
                }

                paramList.addAll(cond.getSql().getParams());
            }

            // sort by find in set
            if (!inConditionList.isEmpty() && inConditionList.size() == 1) {
                Condition inCondition = inConditionList.get(0);
                findInSet.append("find_in_set(").append(inCondition.getField()).append(",'");
                for (Object param : inCondition.getSql().getParams()) {
                    findInSet.append(param).append(',');
                }
                findInSet.deleteCharAt(findInSet.length() - 1);
                findInSet.append("')");
            }
        }

        // 添加排序条件
        if (orderList != null && !orderList.isEmpty()) {
            orderSql.append(" order by ");
            for (OrderBy orderBy : orderList) {
                orderSql.append('`').append(orderBy.getField()).append('`');
                orderSql.append(" ");
                orderSql.append(orderBy.getOrder().getValue());
                orderSql.append(",");
            }
            orderSql.deleteCharAt(orderSql.length() - 1);
        } else if (StringUtil.isNotBlank(findInSet.toString())) {
            orderSql.append(" order by ");
            orderSql.append(findInSet);
        }

        sqlText.append(whereSql);
        sqlText.append(orderSql);
        // 添加分页
        if (start > -1 && limit > -1) {
            sqlText.append(" limit ?,?");
            paramList.add(start);
            paramList.add(limit);
        } else if (limit != -1) {
            sqlText.append(" limit ?");
            paramList.add(limit);
        }

        return SQL.valueOf(sqlText.toString(), paramList);
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
        if (StringUtil.isNotBlank(getWhereSql().getSql())) {
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
    public static class OrderBy {
        private String   field;
        private Class<?> fieldType;
        private Order    order;

        public OrderBy(String field, Order order, Class<?> clazz) {
            Field f = BeansUtil.getField(clazz, field);

            if (clazz != null)
                this.field = BeansUtil.getFieldName(f);
            else
                this.field = field;

            this.fieldType = f.getType();
            this.order = order;
        }

        public String getField() {
            return field;
        }

        public Class<?> getFieldType() {
            return this.fieldType;
        }

        public Order getOrder() {
            return order;
        }

        @Override
        public String toString() {
            return "OrderBy [field=" + field + ", fieldType=" + fieldType + ", order=" + order + "]";
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
