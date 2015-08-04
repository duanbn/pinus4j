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

package org.pinus4j.cluster;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.entity.meta.DBTable;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.generator.IIdGenerator;

/**
 * 数据库集群. 数据库集群主要类，持有所有的数据库集群信息，保存集群的数据库连接包括主库和从库。 初始化集群的方法，<br/>
 * 当没有设置scanpakcage时，从zookeeper中加载. 已DbcpDBClusterImpl实现为例<br/>
 * 
 * <pre>
 * IDBCluster dbCluster = new DbcpDBClusterImpl(); </br>
 * dbCluster.setDbRouteAlg(EnumDBRouteAlg); // 设置分片路由算法. 可选
 * dbCluster.setScanPackage("entity full path package"); // 可选
 * dbCluster.setShardInfoFromZk(true | false); // 默认为false, 可选
 * dbCluster.startup();
 * </pre>
 * 
 * @author duanbn
 */
public interface IDBCluster {

    /**
     * judge this cluster have global slave.
     * 
     * @return
     */
    boolean isGlobalSlaveExist(String clusterName);

    /**
     * judege this cluster have sharding slave.
     * 
     * @param shardingKey
     * @return
     */
    boolean isShardingSlaveExist(String clusterName);

    /**
     * get transaction manager.
     * 
     * @return
     */
    TransactionManager getTransactionManager();

    /**
     * get primary cache ref.
     * 
     * @return primary cache instance.
     */
    IPrimaryCache getPrimaryCache();

    /**
     * get second cache ref.
     * 
     * @return second cache instance.
     */
    ISecondCache getSecondCache();

    /**
     * create a destribute lock by give name.
     *
     * @return destirbute lock.
     */
    Lock createLock(String name);

    /**
     * 设置此集群是否从zookeeper中加载分片信息.
     *
     * @param value true:是， false:否.
     */
    void setShardInfoFromZk(boolean value);

    /**
     * 从Zookeeper中获取分片信息.
     *
     * @return 分片信息.
     */
    List<DBTable> getDBTableFromZk();

    /**
     * 从Jvm中获取分片信息.
     *
     * @return 分片信息.
     */
    List<DBTable> getDBTableFromJvm();

    /**
     * Get all info about this cluster.
     *
     * @return all cluster info.
     */
    Collection<DBClusterInfo> getDBClusterInfo();

    /**
     * 获取集群信息.
     * 
     * @param clusterName 集群名
     * @return 集群信息
     */
    DBClusterInfo getDBClusterInfo(String clusterName);

    /**
     * 启动集群. 调用数据库集群前需要调用此方法，为了初始化集群连接.
     * 
     * @throws DBClusterException 初始化失败
     */
    void startup() throws DBClusterException;

    /**
     * 启动集群. 调用数据库集群前需要调用此方法，为了初始化集群连接.
     * 
     * @param xmlFilePath 配置文件绝对路径
     * @throws DBClusterException 初始化失败
     */
    void startup(String xmlFilePath) throws DBClusterException;

    /**
     * 关闭集群. 系统停止时关闭数据库集群.
     * 
     * @throws DBClusterException 关闭失败
     */
    void shutdown() throws DBClusterException;

    /**
     * 获取主全局库连接.
     * 
     * @param clusterName
     * @return
     */
    IDBResource getMasterGlobalDBResource(String clusterName, String tableName) throws DBClusterException;

    /**
     * 获取从库的全局库连接
     * 
     * @param clusterName
     * @param slave
     * @return
     */
    IDBResource getSlaveGlobalDBResource(String clusterName, String tableName, EnumDBMasterSlave slave)
            throws DBClusterException;

    /**
     * 从主库集群中获取被操作的库表.
     * 
     * @param tableName 数据表名
     * @param value 分库分表因子.
     * @return 被操作的库表
     */
    IDBResource selectDBResourceFromMaster(String tableName, IShardingKey<?> value) throws DBClusterException;

    /**
     * 从从库集群中获取被操作的库表.
     * 
     * @param slave 从库
     * @param tableName 数据库表名
     * @param value 分库分表因子
     * @return 被操作的库表
     */
    IDBResource selectDBResourceFromSlave(String tableName, IShardingKey<?> value, EnumDBMasterSlave slave)
            throws DBClusterException;

    /**
     * 获取此实体对象对应的所有的分库分表引用.
     * 
     * @param clazz 数据对象
     * @return
     */
    List<IDBResource> getAllMasterShardingDBResource(Class<?> clazz) throws SQLException, SystemException;

    /**
     * get all master sharding info.
     * 
     * @param tableNum
     * @param clusterName
     * @param tableName
     * @return
     */
    List<IDBResource> getAllMasterShardingDBResource(int tableNum, String clusterName, String tableName)
            throws SQLException, SystemException;

    /**
     * 获取集群从库列表.
     * 
     * @param clazz 数据对象
     * @param slave 从库号
     */
    List<IDBResource> getAllSlaveShardingDBResource(Class<?> clazz, EnumDBMasterSlave slave) throws SQLException,
            DBClusterException, SystemException;

    /**
     * 设置数据表同步动作.
     * 
     * @param syncAction
     */
    void setSyncAction(EnumSyncAction syncAction);

    /**
     * 获取id生成器.
     * 
     * @return
     */
    IIdGenerator getIdGenerator();

    /**
     * 设置需要扫描的实体对象包.
     * 
     * @param scanPackage 包名
     */
    void setScanPackage(String scanPackage);

    /**
     * 获取集群表集合.
     * 
     * @return 集群表集合
     */
    ITableCluster getTableCluster();

}
