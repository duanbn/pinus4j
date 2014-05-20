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
 * 数据库时间戳.
 *
 * @author duanbn
 */
public @interface UpdateTime {
	
	/**
	 * 注释
	 */
	String comment() default "";
	
}
