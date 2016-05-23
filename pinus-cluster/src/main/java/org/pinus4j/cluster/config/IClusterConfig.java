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

package org.pinus4j.cluster.config;

import java.util.Collection;

import org.pinus4j.cache.beans.PrimaryCacheInfo;
import org.pinus4j.cache.beans.SecondCacheInfo;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.cluster.enums.HashAlgoEnum;

/**
 * 存储中间件配置信息接口. 此接口提供的信息都是通过配置来获取.
 * 一个storage-config.xml中可以配置多个数据库集群，每个数据库集群又可以配置一个主库集群和多个从库集群.
 * 
 * @author duanbn
 * @since 0.1.0
 */
public interface IClusterConfig {

    /*************************************************************
     * 默认实现
     *************************************************************/
    /**
     * the class of default connection pool implement.
     */
    public static final String DEFAULT_CP_CLASS             = "org.pinus4j.cluster.cp.impl.DBCPConnectionPoolImpl";

    /**
     * the class of default primary cache implement.
     */
    public static final String DEFAULT_PRIMARY_CACHE_CLASS  = "org.pinus4j.cache.impl.MemCachedPrimaryCacheImpl";

    /**
     * the class of default second cache implement.
     */
    public static final String DEFAULT_SECOND_CACHE_CLASS   = "org.pinus4j.cache.impl.MemCachedSecondCacheImpl";

    /**
     * the class of default cluster router implement.
     */
    public static final String DEFAULT_CLUSTER_ROUTER_CLASS = "org.pinus4j.cluster.router.impl.SimpleHashClusterRouter";

    /*************************************************************
     * 配置参数
     *************************************************************/
    /**
     * get connection pool implement class.
     * 
     * @return
     */
    public IDBConnectionPool getImplConnectionPool();

    /**
     * ture is enabled, false is not.
     */
    public boolean isCacheEnabled();

    /**
     * get info of primary cache.
     * 
     * @return
     */
    public PrimaryCacheInfo getPrimaryCacheInfo();

    /**
     * get info of second cache.
     * 
     * @return
     */
    public SecondCacheInfo getSecondCacheInfo();

    /**
     * 获取ID生成器默认批量生成值.
     * 
     * @return
     */
    public int getIdGeneratorBatch();

    /**
     * 获取配置的hash算法.
     * 
     * @return hash算法枚举
     */
    public HashAlgoEnum getHashAlgo();

    /**
     * 获取DB集群信息
     * 
     * @return
     */
    public Collection<DBClusterInfo> getDBClusterInfos();

    /**
     * 获取xml中配置的zookeeper连接
     * 
     * @return
     */
    public String getZookeeperUrl();

}
