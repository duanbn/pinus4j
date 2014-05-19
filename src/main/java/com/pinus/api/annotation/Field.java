package com.pinus.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
/**
 * 字段注解
 *
 * @author duanbn
 */
public @interface Field {

	/**
	 * 字段是否为null
	 */
	boolean isCanNull() default false;

	/**
	 * 字段长度
	 */
	int length() default 0;

	/**
	 * 是否有默认值
	 */
	boolean hasDefault() default true;

	/**
	 * 注释
	 */
	String comment() default "";

}
