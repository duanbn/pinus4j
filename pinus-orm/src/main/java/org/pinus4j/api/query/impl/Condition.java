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

import java.lang.reflect.Array;
import java.util.List;
import java.util.Set;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.impl.DefaultQueryImpl.ConditionRelation;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;

import com.google.common.collect.Lists;

/**
 * 查询条件.
 *
 * @author duanbn
 */
public class Condition {

    /**
     * 条件字段.
     */
    private String            field;
    /**
     * 条件值.
     */
    private Object            value;
    /**
     * 条件枚举.
     */
    private QueryOpt          opt;

    /**
     * 保存or查询条件.
     */
    private Condition[]       orCond;

    /**
     * 保存and查询条件.
     */
    private Condition[]       andCond;

    /**
     * 在一个Query中的条件
     */
    private ConditionRelation conditionRelation;

    /**
     * 构造方法. 防止调用者直接创建此对象.
     */
    private Condition() {
    }

    /**
     * 构造方法.
     *
     * @param field 条件字段
     * @param value 条件值
     * @param opt 条件枚举
     */
    private Condition(String field, Object value, QueryOpt opt, Class<?> clazz) {
        if (StringUtil.isBlank(field)) {
            throw new IllegalArgumentException("条件字段不能为空, condition field=" + field);
        }
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=" + value);
        }
        if (value.getClass().isArray() && Array.getLength(value) == 0) {
            throw new IllegalArgumentException("参数错误, condition value是数组并且数组长度为0");
        }

        this.field = field;
        if (clazz != null)
            this.field = BeansUtil.getFieldName(BeansUtil.getField(clazz, field));

        this.value = SQLBuilder.formatValue(value);
        this.opt = opt;
    }

    /**
     * 构造方法.null or not null
     *
     * @param field 条件字段
     * @param opt 条件枚举
     */
    private Condition(String field, QueryOpt opt, Class<?> clazz) {
        if (StringUtil.isBlank(field)) {
            throw new IllegalArgumentException("条件字段不能为空, condition field=" + field);
        }

        this.field = field;
        if (clazz != null)
            this.field = BeansUtil.getFieldName(BeansUtil.getField(clazz, field));
        this.opt = opt;
    }

    /**
     * 返回当前条件对象表示的sql语句.
     *
     * @return sql语句
     */
    SQL getSql() {
        StringBuilder sqlText = new StringBuilder();
        if (orCond != null && orCond.length > 0) {
            List<Object> paramList = Lists.newArrayList();
            sqlText.append("(");
            for (Condition cond : orCond) {
                sqlText.append(cond.getSql().getSql()).append(" or ");
                paramList.addAll(cond.getSql().getParams());
            }
            sqlText.delete(sqlText.lastIndexOf(" or "), sqlText.length());
            sqlText.append(")");
            return SQL.valueOf(sqlText.toString(), paramList.toArray(new Object[paramList.size()]));
        } else if (andCond != null && andCond.length > 0) {
            List<Object> paramList = Lists.newArrayList();
            sqlText.append("(");
            for (Condition cond : andCond) {
                sqlText.append(cond.getSql().getSql()).append(" and ");
                paramList.addAll(cond.getSql().getParams());
            }
            sqlText.delete(sqlText.lastIndexOf(" and "), sqlText.length());
            sqlText.append(")");
            return SQL.valueOf(sqlText.toString(), paramList.toArray(new Object[paramList.size()]));
        } else {
            sqlText.append('`').append(field).append('`').append(" ").append(opt.getSymbol()).append(" ");
            List<Object> paramList = Lists.newArrayList();
            switch (opt) {
                case IN:
                    int paramLength = Array.getLength(this.value);

                    sqlText.append("(");
                    for (int i = 0; i < paramLength; i++) {
                        sqlText.append('?').append(',');

                        Object val = Array.get(this.value, i);

                        Class<?> clazz = val.getClass();
                        if (clazz == Boolean.class || clazz == Boolean.TYPE) {
                            if ((Boolean) val) {
                                paramList.add("1");
                            } else {
                                paramList.add("0");
                            }
                        } else {
                            paramList.add(val);
                        }
                    }
                    sqlText.deleteCharAt(sqlText.length() - 1);
                    sqlText.append(")");

                    break;
                case ISNULL:
                    break;
                case ISNOTNULL:
                    break;
                default:
                    sqlText.append('?');
                    Object value = this.value;
                    if (value instanceof Boolean) {
                        if ((Boolean) value) {
                            paramList.add("1");
                        } else {
                            paramList.add("0");
                        }
                    } else {
                        paramList.add(value);
                    }
                    break;
            }

            return SQL.valueOf(sqlText.toString(), paramList);
        }
    }

    @Override
    public String toString() {
        return getSql().toString();
    }

    public static Condition eq(String field, Object value) {
        return eq(field, value, null);
    }

    /**
     * 等于条件.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition eq(String field, Object value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.EQ, clazz);
        return cond;
    }

    public static Condition noteq(String field, Object value) {
        return noteq(field, value, null);
    }

    /**
     * 不等于条件.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition noteq(String field, Object value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.NOTEQ, clazz);
        return cond;
    }

    public static Condition gt(String field, Object value) {
        return gt(field, value, null);
    }

    /**
     * 大于条件.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition gt(String field, Object value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.GT, clazz);
        return cond;
    }

    public static Condition gte(String field, Object value) {
        return gte(field, value, null);
    }

    /**
     * 大于等于条件.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition gte(String field, Object value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.GTE, clazz);
        return cond;
    }

    public static Condition lt(String field, Object value) {
        return lt(field, value, null);
    }

    /**
     * 小于条件.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition lt(String field, Object value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.LT, clazz);
        return cond;
    }

    public static Condition lte(String field, Object value) {
        return lte(field, value, null);
    }

    /**
     * 小于等于条件.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition lte(String field, Object value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.LTE, clazz);
        return cond;
    }

    public static Condition in(String field, List<? extends Object> values) {
        return in(field, values, null);
    }

    public static Condition in(String field, List<? extends Object> values, Class<?> clazz) {
        return in(field, clazz, values.toArray(new Object[values.size()]));
    }

    public static Condition in(String field, Set<? extends Object> values) {
        return in(field, values, null);
    }

    public static Condition in(String field, Set<? extends Object> values, Class<?> clazz) {
        return in(field, clazz, values.toArray(new Object[values.size()]));
    }

    public static Condition in(String field, Object... values) {
        return in(field, null, values);
    }

    /**
     * in操作.
     *
     * @param field 条件字段
     * @param values 字段值
     * @return 当前条件对象
     */
    public static Condition in(String field, Class<?> clazz, Object... values) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, byte[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, byte[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, int[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, int[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, short[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, short[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, long[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, long[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, float[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, float[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, double[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, double[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, boolean[] values) {
        return in(field, values, null);
    }

    public static Condition in(String field, boolean[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition like(String field, String value) {
        return like(field, value, null);
    }

    /**
     * like查询.
     *
     * @param field 条件字段
     * @param value 字段值
     */
    public static Condition like(String field, String value, Class<?> clazz) {
        if (value == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, value, QueryOpt.LIKE, clazz);
        return cond;
    }

    /**
     * or查询.
     *
     * @param conds 查询条件
     */
    public static Condition or(Condition... conds) {
        if (conds == null || conds.length < 2) {
            throw new IllegalArgumentException("参数错误, or查询条件最少为2个");
        }
        Condition cond = new Condition();
        cond.setOrCond(conds);
        return cond;
    }

    /**
     * and查询
     * 
     * @param conds
     * @return
     */
    public static Condition and(Condition... conds) {
        if (conds == null || conds.length < 2) {
            throw new IllegalArgumentException("参数错误, and查询条件最少为2个");
        }
        Condition cond = new Condition();
        cond.setAndCond(conds);
        return cond;
    }

    public static Condition isNull(String field) {
        return isNull(field, null);
    }

    /**
     * 为null 查询.
     *
     * @param field 条件字段
     * @param clazz class 查询条件
     */
    public static Condition isNull(String field, Class<?> clazz) {
        Condition cond = new Condition(field, QueryOpt.ISNULL, clazz);
        return cond;
    }

    public static Condition isNotNull(String field) {
        return isNotNull(field, null);
    }

    /**
     * 不为null查询.
     *
     * @param field 条件字段
     * @param clazz class 查询条件
     */
    public static Condition isNotNull(String field, Class<?> clazz) {
        Condition cond = new Condition(field, QueryOpt.ISNOTNULL, clazz);
        return cond;
    }

    boolean isAndCondAllEQ() {
        if (andCond.length == 0) {
            return false;
        }

        for (Condition oneAnd : andCond) {
            if (oneAnd.getOpt() != QueryOpt.EQ) {
                return false;
            }
        }

        return true;
    }

    String getField() {
        return field;
    }

    void setField(String field) {
        this.field = field;
    }

    Object getValue() {
        return value;
    }

    void setValue(Object value) {
        this.value = value;
    }

    QueryOpt getOpt() {
        return opt;
    }

    void setOpt(QueryOpt opt) {
        this.opt = opt;
    }

    Condition[] getOrCond() {
        return orCond;
    }

    void setOrCond(Condition[] orCond) {
        this.orCond = orCond;
    }

    Condition[] getAndCond() {
        return andCond;
    }

    void setAndCond(Condition[] andCond) {
        this.andCond = andCond;
    }

    ConditionRelation getConditionRelation() {
        return conditionRelation;
    }

    void setConditionRelation(ConditionRelation conditionRelation) {
        this.conditionRelation = conditionRelation;
    }
}
