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

import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.utils.ReflectUtil;
import org.pinus4j.utils.StringUtils;

/**
 * 查询条件.
 *
 * @author duanbn
 */
public class Condition {

    /**
     * 条件字段.
     */
    private String      field;
    /**
     * 条件值.
     */
    private Object      value;
    /**
     * 条件枚举.
     */
    private QueryOpt    opt;

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
     * @param field 条件字段
     * @param value 条件值
     * @param opt 条件枚举
     */
    private Condition(String field, Object value, QueryOpt opt, Class<?> clazz) {
        if (StringUtils.isBlank(field)) {
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
            this.field = ReflectUtil.getFieldName(ReflectUtil.getField(clazz, field));

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
        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("条件字段不能为空, condition field=" + field);
        }

        this.field = field;
        if (clazz != null)
            this.field = ReflectUtil.getFieldName(ReflectUtil.getField(clazz, field));
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
                    for (int i = 0; i < Array.getLength(this.value); i++) {
                        Object val = Array.get(this.value, i);
                        Class<?> clazz = val.getClass();
                        if (clazz == String.class) {
                            SQL.append("'").append(val).append("'");
                        } else if (clazz == Boolean.class || clazz == Boolean.TYPE) {
                            if ((Boolean) val) {
                                SQL.append("'").append("1").append("'");
                            } else {
                                SQL.append("'").append("0").append("'");
                            }
                        } else {
                            SQL.append(val);
                        }
                        SQL.append(",");
                    }
                    SQL.deleteCharAt(SQL.length() - 1);
                    SQL.append(")");

                    break;
                case ISNULL:
                    break;
                case ISNOTNULL:
                    break;
                default:
                    Object value = this.value;
                    if (value instanceof String) {
                        SQL.append(value);
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
        return gte(field, value);
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
        return in(field, values);
    }

    public static Condition in(String field, short[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, long[] values) {
        return in(field, values);
    }

    public static Condition in(String field, long[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, float[] values) {
        return in(field, values);
    }

    public static Condition in(String field, float[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, double[] values) {
        return in(field, values);
    }

    public static Condition in(String field, double[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition in(String field, boolean[] values) {
        return in(field, values);
    }

    public static Condition in(String field, boolean[] values, Class<?> clazz) {
        if (values == null) {
            throw new IllegalArgumentException("参数错误, condition value=null");
        }
        Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
        return cond;
    }

    public static Condition like(String field, String value) {
        return like(field, value);
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
     * 或查询.
     *
     * @param conds 查询条件
     */
    public static Condition or(Condition... conds) {
        if (conds == null || conds.length < 2) {
            throw new IllegalArgumentException("参数错误, or查询条件最少为2个");
        }
        Condition cond = new Condition(conds);
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
}
