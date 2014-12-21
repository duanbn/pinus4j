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

package org.pinus.constant;

/**
 * 系统常量.
 * 
 * @author duanbn
 */
public class Const {

	//
	// zookeeper目录相关常量.
	//
	public static final String ZK_ROOT = "/pinus";
	/**
	 * 每个表的分库分表信息的根目录. 每个分表信息保存在此目录下的文件.
	 */
	public static final String ZK_SHARDINGINFO = ZK_ROOT + "/shardinginfo";
	/**
	 * id生成器根目录. 每一个表的id当前id保存在此目录下相关文件.
	 */
	public static final String ZK_PRIMARYKEY = ZK_ROOT + "/primarykey";

	/**
	 * 分布式锁目录
	 */
	public static final String ZK_LOCKS = ZK_ROOT + "/locks";

	/**
	 * 字符串超过此值则转换为Text
	 */
	public static final int COLUMN_TEXT_LENGTH = 4000;

	//
	// SQL相关
	//
	/**
	 * 查询count的慢日志时间阈值
	 */
	public static final int SLOWQUERY_COUNT = 2000;
	/**
	 * 遍历表慢查询时间阈值
	 */
	public static final int SLOWQUERY_MORE = 100;
	/**
	 * 根据Query对象查询的慢日志时间阈值
	 */
	public static final int SLOWQUERY_QUERY = 50;
	/**
	 * 根据SQL对象查询的慢日志时间阈值
	 */
	public static final int SLOWQUERY_SQL = 50;
	/**
	 * 根据主键查询的慢日志时间阈值
	 */
	public static final int SLOWQUERY_PK = 1;
	/**
	 * 根据多主键查询的慢日志时间阈值
	 */
	public static final int SLOWQUERY_PKS = 10;

	//
	// 配置文件相关常量.
	//
	/**
	 * 默认读取的配置文件名.
	 */
	public static final String DEFAULT_CONFIG_FILENAME = "storage-config.xml";

	public static final String PROP_IDGEN_BATCH = "db.cluster.generateid.batch";

	public static final String PROP_HASH_ALGO = "db.cluster.hash.algo";

	/**
	 * zookeeper连接地址
	 */
	public static final String PROP_ZK_URL = "db.cluster.zk";

	// dbcp连接池
	public static final String PROP_MAXACTIVE = "maxActive";
	public static final String PROP_MINIDLE = "minIdle";
	public static final String PROP_MAXIDLE = "maxIdle";
	public static final String PROP_INITIALSIZE = "initialSize";
	public static final String PROP_REMOVEABANDONED = "removeAbandoned";
	public static final String PROP_REMOVEABANDONEDTIMEOUT = "removeAbandonedTimeout";
	public static final String PROP_MAXWAIT = "maxWait";
	public static final String PROP_TIMEBETWEENEVICTIONRUNSMILLIS = "timeBetweenEvictionRunsMillis";
	public static final String PROP_NUMTESTSPEREVICTIONRUN = "numTestsPerEvictionRun";
	public static final String PROP_MINEVICTABLEIDLETIMEMILLIS = "minEvictableIdleTimeMillis";

	//
	// 系统变量相关常量.
	//
	/**
	 * zookeeper连接信息. -Dstorage.zkhost=
	 */
	public static final String SYSTEM_PROPERTY_ZKHOST = "storage.zkhost";

	//
	// 集群相关常量.
	//
	public static final byte MSTYPE_MASTER = 0;
	public static final byte MSTYPE_SLAVE = 1;

	// 数据类型
	public static final String TRUE = "1";
	public static final String FALSE = "0";

}
