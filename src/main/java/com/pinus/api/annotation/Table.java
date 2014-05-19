package com.pinus.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
/**
 * 数据表注解.
 *
 * @author duanbn
 */
public @interface Table {

	/**
	 * 数据表名
	 * 
	 * @return
	 */
	String name() default "";

	/**
	 * 集群名称
	 */
	String cluster();

	/**
	 * 分表数
	 */
	int shardingNum() default 0;

	/**
	 * 此表是否需要被缓存.
	 * 
	 * @return true:是， false:否
	 */
	boolean cache() default false;

}
