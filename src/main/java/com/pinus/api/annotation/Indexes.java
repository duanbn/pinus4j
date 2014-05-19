package com.pinus.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
/**
 * 表示一个表的主键.
 * 一个表可以定义多个字段的索引.
 *
 * @author duanbn
 */
public @interface Indexes {

    /**
     * 索引数组
     */
    Index[] value();

}
