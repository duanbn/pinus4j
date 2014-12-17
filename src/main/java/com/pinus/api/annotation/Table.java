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
	 * 基于哪个字段进行分片
	 */
	String shardingBy() default "";

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
