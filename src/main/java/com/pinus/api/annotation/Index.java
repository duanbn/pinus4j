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
 * 表示一个字段的索引.
 * 如果联合索引可以使用逗号分隔多个字段
 * 例如<pre>@Index(field="fieldName1,fieldName2")</pre>
 *
 * @author duanbn
 */
public @interface Index {

    /**
     * 是否是唯一索引.
     */
    boolean isUnique() default false;

    /**
     * 需要被索引的字段
     */
    String field();
  
}
