package com.pinus.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
/**
 * 数据库日期注解.
 *
 * @author duanbn
 */
public @interface DateTime {

    /**
     * 是否为null
     */
    boolean isCanNull() default false;

    /**
     * 是否有默认值
     */
    boolean hasDefault() default true;
    
    /**
	 * 注释
	 */
	String comment() default "";

}
