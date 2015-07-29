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

package org.pinus4j.cluster.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.pinus4j.cache.ICacheBuilder;
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cache.impl.DefaultCacheBuilder;
import org.pinus4j.cluster.DefaultContainerFactory;
import org.pinus4j.cluster.DefaultContainerFactory.ContainerType;
import org.pinus4j.cluster.IContainer;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.ITableCluster;
import org.pinus4j.cluster.ITableClusterBuilder;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.DBRegionInfo;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.config.impl.XmlClusterConfigImpl;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.cluster.resources.DBResourceCache;
import org.pinus4j.cluster.resources.GlobalDBResource;
import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.cluster.router.IClusterRouter;
import org.pinus4j.cluster.router.IClusterRouterBuilder;
import org.pinus4j.cluster.router.RouteInfo;
import org.pinus4j.cluster.router.impl.DefaultClusterRouterBuilder;
import org.pinus4j.constant.Const;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.DBTable;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.exceptions.DBRouteException;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.generator.DefaultDBGeneratorBuilder;
import org.pinus4j.generator.IDBGenerator;
import org.pinus4j.generator.IDBGeneratorBuilder;
import org.pinus4j.generator.IIdGenerator;
import org.pinus4j.generator.impl.DistributedSequenceIdGeneratorImpl;
import org.pinus4j.transaction.impl.BestEffortsOnePCJtaTransactionManager;
import org.pinus4j.utils.CuratorDistributeedLock;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.JdbcUtil;
import org.pinus4j.utils.BeanUtil;
import org.pinus4j.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抽象数据库集群. 主要负责初始化数据库集群的数据源对象、分表信息.<br/>
 * need to invoke startup method before use it, invoke shutdown method at last.
 * 
 * @author duanbn
 */
public abstract class AbstractDBCluster implements IDBCluster {

    /**
     * 日志
     */
    private static final Logger        LOG        = LoggerFactory.getLogger(AbstractDBCluster.class);

    public static final Random         r          = new Random();

    /**
     * 同步数据表操作.
     */
    private EnumSyncAction             syncAction = EnumSyncAction.CREATE;

    /**
     * 扫描数据对象包.
     */
    private String                     scanPackage;

    /**
     * 数据分片信息是否从zookeeper中获取.
     */
    private boolean                    isShardInfoFromZk;

    /**
     * 数据库类型.
     */
    protected EnumDB                   enumDb     = EnumDB.MYSQL;

    /**
     * Entity管理器
     */
    private IEntityMetaManager         entityManager;

    /**
     * 数据库表生成器.
     */
    private IDBGenerator               dbGenerator;

    /**
     * 主键生成器. 默认使用SimpleIdGeneratorImpl生成器.
     */
    private IIdGenerator               idGenerator;

    /**
     * 一级缓存.
     */
    private IPrimaryCache              primaryCache;

    /**
     * 二级缓存.
     */
    private ISecondCache               secondCache;

    /**
     * 事务管理器
     */
    private TransactionManager         txManager;

    /**
     * cluster router.
     */
    private IContainer<IClusterRouter> dbRouterC;

    /**
     * cluster info container.
     */
    private IContainer<DBClusterInfo>  dbClusterInfoC;

    /**
     * 集群中的表集合.
     */
    private ITableCluster              tableCluster;

    /**
     * 集群配置.
     */
    private IClusterConfig             config;

    /**
     * curator client.
     */
    private CuratorFramework           curatorClient;

    /**
     * 构造方法.
     * 
     * @param enumDb 数据库类型.
     */
    public AbstractDBCluster(EnumDB enumDb) {
        this.enumDb = enumDb;
    }

    @Override
    public Collection<DBClusterInfo> getDBClusterInfo() {
        return this.dbClusterInfoC.values();
    }

    @Override
    public DBClusterInfo getDBClusterInfo(String clusterName) {
        DBClusterInfo clusterInfo = this.dbClusterInfoC.find(clusterName);

        if (clusterInfo == null) {
            throw new DBOperationException("找不到集群信息, clusterName=" + clusterName);
        }

        return clusterInfo;
    }

    @Override
    public void startup() throws DBClusterException {
        startup(null);
    }

    @Override
    public void startup(String xmlFilePath) throws DBClusterException {
        LOG.info("start init database cluster");

        // load storage-config.xml
        try {
            config = _getConfig(xmlFilePath);
        } catch (LoadConfigException e) {
            throw new RuntimeException(e);
        }

        // init curator framework
        this.curatorClient = CuratorFrameworkFactory.newClient(config.getZookeeperUrl(), new RetryNTimes(5, 1000));
        this.curatorClient.start();

        try {
            // create zookeeper root dir
            ZooKeeper zkClient = this.curatorClient.getZookeeperClient().getZooKeeper();
            Stat stat = zkClient.exists(Const.ZK_ROOT, false);
            if (stat == null) {
                zkClient.create(Const.ZK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new IllegalStateException("初始化zookeeper根目录失败");
        }

        // init entity manager
        this.entityManager = DefaultEntityMetaManager.getInstance();

        //
        // init id generator
        //
        this.idGenerator = new DistributedSequenceIdGeneratorImpl(config, this.curatorClient);
        LOG.info("init primary key generator done");

        //
        // init db cache
        //
        ICacheBuilder cacheBuilder = DefaultCacheBuilder.valueOf(config);
        // find primary cache
        this.primaryCache = cacheBuilder.buildPrimaryCache();

        // find second cache
        this.secondCache = cacheBuilder.buildSecondCache();

        // init transaction manager
        this.txManager = BestEffortsOnePCJtaTransactionManager.getInstance();

        //
        // init db generator
        //
        IDBGeneratorBuilder dbGeneratorBuilder = DefaultDBGeneratorBuilder.valueOf(this.syncAction, this.enumDb);
        this.dbGenerator = dbGeneratorBuilder.build();

        //
        // load db cluster info.
        //
        Collection<DBClusterInfo> dbClusterInfos = config.getDBClusterInfos();
        try {
            // 初始化主集群连接
            _initDBCluster(dbClusterInfos);

            // 初始化数据表集群信息.
            List<DBTable> tables = null;
            if (isShardInfoFromZk) {
                // get table sharding info from zookeeper
                tables = getDBTableFromZk();
            } else {
                if (StringUtils.isBlank(scanPackage)) {
                    throw new DBClusterException(
                            "get shardinfo from jvm, but i can't find scanpackage full path, did you forget setScanPackage ?");
                }

                // get table sharding info from jvm
                tables = getDBTableFromJvm();
                // 表分片信息写入zookeeper
                _syncToZookeeper(tables);
            }
            if (tables.isEmpty()) {
                throw new DBClusterException("找不到可以创建库表的实体对象, package=" + scanPackage);
            }

            // init table cluster.
            ITableClusterBuilder tableClusterBuilder = NumberIndexTableClusterBuilder.valueOf(tables);
            this.tableCluster = tableClusterBuilder.build();

            // create db tables.
            if (this.syncAction != EnumSyncAction.NONE) {
                LOG.info("syncing table info");
                long start = System.currentTimeMillis();
                _createTable(tables);
                LOG.info("sync table info done, const time:" + (System.currentTimeMillis() - start) + "ms");
            }

        } catch (Exception e) {
            throw new DBClusterException("init database cluster failure", e);
        }

        //
        // init db router
        //
        this.dbRouterC = DefaultContainerFactory.createContainer(ContainerType.MAP);
        IClusterRouterBuilder routerBuilder = null;
        String clusterName = null;
        for (DBClusterInfo clusterInfo : this.dbClusterInfoC.values()) {
            clusterName = clusterInfo.getClusterName();
            routerBuilder = DefaultClusterRouterBuilder.valueOf(this);
            routerBuilder.setHashAlgo(config.getHashAlgo());
            this.dbRouterC.add(clusterName, routerBuilder.build(clusterName));
        }
        LOG.info("init database cluster done.");
    }

    /**
     * relase resource. include database connection,zookeeper connection and
     * cache connection.
     */
    @Override
    public void shutdown() throws DBClusterException {

        // close cache connection
        if (this.primaryCache != null)
            this.primaryCache.close();
        if (this.secondCache != null)
            this.secondCache.close();

        try {
            // close database connection
            for (DBClusterInfo dbClusterInfo : this.dbClusterInfoC.values()) {
                // 关闭全局库
                // 主全局库
                DBInfo masterGlobal = dbClusterInfo.getMasterGlobalDBInfo();
                if (masterGlobal != null)
                    closeDataSource(masterGlobal);

                // 从全局库
                List<DBInfo> slaveDbs = dbClusterInfo.getSlaveGlobalDBInfo();
                if (slaveDbs != null && !slaveDbs.isEmpty()) {
                    for (DBInfo slaveGlobal : slaveDbs) {
                        closeDataSource(slaveGlobal);
                    }
                }

                // 关闭集群库
                for (DBRegionInfo regionInfo : dbClusterInfo.getDbRegions()) {
                    // 主集群
                    for (DBInfo dbConnInfo : regionInfo.getMasterDBInfos()) {
                        closeDataSource(dbConnInfo);
                    }

                    // 从集群
                    for (List<DBInfo> dbConnInfos : regionInfo.getSlaveDBInfos()) {
                        for (DBInfo dbConnInfo : dbConnInfos) {
                            closeDataSource(dbConnInfo);
                        }
                    }
                }
            }

        } catch (Exception e) {
            throw new DBClusterException("关闭数据库集群失败", e);
        }

        // clear resource cache.
        DBResourceCache.clear();

        // close curator
        CloseableUtils.closeQuietly(this.curatorClient);
    }

    @Override
    public IDBResource getMasterGlobalDBResource(String clusterName, String tableName) throws DBClusterException {
        DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
        if (dbClusterInfo == null) {
            throw new DBClusterException("没有找到集群信息, clustername=" + clusterName);
        }

        DBInfo masterDBInfo = dbClusterInfo.getMasterGlobalDBInfo();
        if (masterDBInfo == null) {
            throw new DBClusterException("此集群没有配置全局主库, clustername=" + clusterName);
        }

        IDBResource masterDBResource;
        try {
            masterDBResource = GlobalDBResource.valueOf(masterDBInfo, tableName);
        } catch (SQLException e) {
            throw new DBClusterException(e);
        }

        return masterDBResource;
    }

    @Override
    public IEntityMetaManager getEntityManager() {
        return this.entityManager;
    }

    @Override
    public IDBResource getSlaveGlobalDBResource(String clusterName, String tableName, EnumDBMasterSlave masterSlave)
            throws DBClusterException {
        DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
        if (dbClusterInfo == null) {
            throw new DBClusterException("没有找到集群信息, clustername=" + clusterName);
        }

        List<DBInfo> slaveDbs = dbClusterInfo.getSlaveGlobalDBInfo();
        if (slaveDbs == null || slaveDbs.isEmpty()) {
            throw new DBClusterException("此集群没有配置全局从库, clustername=" + clusterName);
        }

        DBInfo slaveDBInfo = null;
        if (EnumDBMasterSlave.AUTO == masterSlave) {
            // random select
            slaveDBInfo = slaveDbs.get(r.nextInt(slaveDbs.size() - 1));
        } else {
            slaveDBInfo = slaveDbs.get(masterSlave.getValue());
        }

        IDBResource slaveDBResource;
        try {
            slaveDBResource = GlobalDBResource.valueOf(slaveDBInfo, tableName);
        } catch (SQLException e) {
            throw new DBClusterException(e);
        }

        return slaveDBResource;
    }

    @Override
    public ShardingDBResource selectDBResourceFromMaster(String tableName, IShardingKey<?> value)
            throws DBClusterException {

        // 计算分库
        // 计算路由信息
        RouteInfo routeInfo = null;
        try {
            String clusterName = value.getClusterName();

            IClusterRouter router = this.dbRouterC.find(clusterName);
            if (router == null) {
                throw new IllegalStateException("can not found db router by " + clusterName);
            }

            routeInfo = router.select(EnumDBMasterSlave.MASTER, tableName, value);
        } catch (DBRouteException e) {
            throw new DBClusterException(e);
        }
        String clusterName = routeInfo.getClusterName();
        DBInfo dbInfo = routeInfo.getDbInfo();
        int tableIndex = routeInfo.getTableIndex();

        // 获取连接信息
        DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
        if (dbClusterInfo == null) {
            throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName);
        }
        DBRegionInfo regionInfo = dbClusterInfo.getDbRegions().get(routeInfo.getRegionIndex());
        if (regionInfo == null) {
            throw new DBClusterException("找不到数据库集群, shardingkey=" + value + ", tablename=" + tableName);
        }

        // 返回分库分表信息
        ShardingDBResource db;
        try {
            db = ShardingDBResource.valueOf(dbInfo, regionInfo, tableName, tableIndex);
        } catch (SQLException e) {
            throw new DBClusterException(e);
        }

        return db;
    }

    @Override
    public ShardingDBResource selectDBResourceFromSlave(String tableName, IShardingKey<?> value,
                                                        EnumDBMasterSlave slaveNum) throws DBClusterException {

        // 计算分库
        // 计算路由信息
        RouteInfo routeInfo = null;
        try {
            String clusterName = value.getClusterName();

            IClusterRouter router = this.dbRouterC.find(clusterName);
            if (router == null) {
                throw new IllegalStateException("can not found db router by " + clusterName);
            }

            routeInfo = router.select(slaveNum, tableName, value);
        } catch (DBRouteException e) {
            throw new DBClusterException(e);
        }
        // 获取分库分表的下标
        String clusterName = routeInfo.getClusterName();
        DBInfo dbInfo = routeInfo.getDbInfo();
        int tableIndex = routeInfo.getTableIndex();

        // 获取连接信息
        DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
        if (dbClusterInfo == null) {
            throw new DBClusterException("can not found db cluster by " + clusterName + ", shardingkey is " + value
                    + ", tablename is " + tableName + ", slavenum is " + slaveNum.getValue());
        }
        DBRegionInfo regionInfo = dbClusterInfo.getDbRegions().get(routeInfo.getRegionIndex());
        if (regionInfo == null) {
            throw new DBClusterException("can not found db cluster by " + clusterName + "shardingkey is " + value
                    + ", tablename is " + tableName + ", slavenum is " + slaveNum.getValue());
        }

        // 返回分库分表信息
        ShardingDBResource db;
        try {
            db = ShardingDBResource.valueOf(dbInfo, regionInfo, tableName, tableIndex);
        } catch (SQLException e) {
            throw new DBClusterException(e);
        }
        return db;
    }

    @Override
    public List<IDBResource> getAllMasterShardingDBResource(Class<?> clazz) throws SQLException {
        int tableNum = BeanUtil.getTableNum(clazz);
        if (tableNum == 0) {
            throw new IllegalStateException("table number is 0");
        }

        String clusterName = BeanUtil.getClusterName(clazz);
        String tableName = BeanUtil.getTableName(clazz);

        return getAllMasterShardingDBResource(tableNum, clusterName, tableName);
    }

    /**
     * get all master sharding info.
     * 
     * @param tableNum
     * @param clusterName
     * @param tableName
     * @return
     * @throws SQLException
     */
    public List<IDBResource> getAllMasterShardingDBResource(int tableNum, String clusterName, String tableName)
            throws SQLException {
        List<IDBResource> dbResources = new ArrayList<IDBResource>();

        if (tableNum == 0) {
            throw new IllegalStateException("table number is 0");
        }

        IDBResource dbResource = null;
        DBClusterInfo dbClusterInfo = this.getDBClusterInfo(clusterName);
        for (DBRegionInfo region : dbClusterInfo.getDbRegions()) {
            for (DBInfo dbInfo : region.getMasterDBInfos()) {
                for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
                    dbResource = ShardingDBResource.valueOf(dbInfo, region, tableName, tableIndex);
                    dbResources.add(dbResource);
                }
            }
        }

        return dbResources;
    }

    @Override
    public List<IDBResource> getAllSlaveShardingDBResource(Class<?> clazz, EnumDBMasterSlave masterSlave)
            throws SQLException, DBClusterException {
        List<IDBResource> dbResources = new ArrayList<IDBResource>();

        int tableNum = BeanUtil.getTableNum(clazz);
        if (tableNum == 0) {
            throw new IllegalStateException("table number is 0");
        }

        String clusterName = BeanUtil.getClusterName(clazz);
        String tableName = BeanUtil.getTableName(clazz);

        IDBResource dbResource = null;
        DBClusterInfo dbClusterInfo = this.getDBClusterInfo(clusterName);
        for (DBRegionInfo region : dbClusterInfo.getDbRegions()) {
            List<DBInfo> slaveDBInfos = null;

            // auto select
            if (EnumDBMasterSlave.AUTO == masterSlave) {
                slaveDBInfos = region.getSlaveDBInfos().get(r.nextInt(region.getSlaveDBInfos().size() - 1));
            } else {
                slaveDBInfos = region.getSlaveDBInfos().get(masterSlave.getValue());
            }

            if (slaveDBInfos == null || slaveDBInfos.isEmpty()) {
                throw new DBClusterException("find slave db cluster failure cluster name is " + clusterName);
            }

            for (DBInfo dbInfo : slaveDBInfos) {
                for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
                    dbResource = ShardingDBResource.valueOf(dbInfo, region, tableName, tableIndex);
                    dbResources.add(dbResource);
                }
            }
        }

        return dbResources;
    }

    @Override
    public boolean isGlobalSlaveExist(String clusterName) {
        DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
        return dbClusterInfo.getSlaveGlobalDBInfo() != null && !dbClusterInfo.getSlaveGlobalDBInfo().isEmpty();
    }

    @Override
    public boolean isShardingSlaveExist(String clusterName) {
        DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);

        List<DBRegionInfo> dbRegions = dbClusterInfo.getDbRegions();

        List<DBInfo> slaveInfos = new ArrayList<DBInfo>();

        for (DBRegionInfo dbRegion : dbRegions) {
            for (List<DBInfo> slaveInfo : dbRegion.getSlaveDBInfos()) {
                slaveInfos.addAll(slaveInfo);
            }
        }

        return !slaveInfos.isEmpty();
    }

    @Override
    public TransactionManager getTransactionManager() {
        return this.txManager;
    }

    @Override
    public IPrimaryCache getPrimaryCache() {
        return this.primaryCache;
    }

    @Override
    public ISecondCache getSecondCache() {
        return this.secondCache;
    }

    @Override
    public Lock createLock(String lockName) {
        InterProcessMutex curatorLock = new InterProcessMutex(curatorClient, Const.ZK_LOCKS + "/" + lockName);
        return new CuratorDistributeedLock(curatorLock);
    }

    @Override
    public void setShardInfoFromZk(boolean value) {
        this.isShardInfoFromZk = value;
    }

    @Override
    public List<DBTable> getDBTableFromZk() {
        List<DBTable> tables = new ArrayList<DBTable>();

        try {
            ZooKeeper zkClient = this.curatorClient.getZookeeperClient().getZooKeeper();

            List<String> zkTableNodes = zkClient.getChildren(Const.ZK_SHARDINGINFO, false);
            byte[] tableData = null;
            for (String zkTableNode : zkTableNodes) {
                tableData = zkClient.getData(Const.ZK_SHARDINGINFO + "/" + zkTableNode, false, null);
                tables.add(IOUtil.getObject(tableData, DBTable.class));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return tables;
    }

    @Override
    public List<DBTable> getDBTableFromJvm() {
        for (String pkgPath : this.scanPackage.split(",")) {
            this.entityManager.loadEntity(pkgPath);
        }

        return this.entityManager.getTableMetaList();
    }

    /**
     * 将表分片信息同步到zookeeper.
     */
    private void _syncToZookeeper(List<DBTable> tables) throws Exception {
        try {
            ZooKeeper zkClient = this.curatorClient.getZookeeperClient().getZooKeeper();

            Stat stat = zkClient.exists(Const.ZK_SHARDINGINFO, false);
            if (stat == null) {
                // 创建根节点
                zkClient.create(Const.ZK_SHARDINGINFO, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            byte[] tableData = null;
            String tableName = null;
            for (DBTable table : tables) {
                tableData = IOUtil.getBytes(table);
                tableName = table.getName();

                // toBeCleanInfo.remove(tableName);

                String zkTableNode = Const.ZK_SHARDINGINFO + "/" + tableName;
                stat = zkClient.exists(zkTableNode, false);
                if (stat == null) {
                    zkClient.create(zkTableNode, tableData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zkClient.setData(zkTableNode, tableData, -1);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("sharding info of tables have flushed to zookeeper done.");
    }

    /**
     * 创建数据库表.
     * 
     * @throws
     * @throws IOException
     */
    private void _createTable(List<DBTable> tables) throws Exception {
        String clusterName = null;
        for (DBTable table : tables) {
            clusterName = table.getCluster();
            if (table.getShardingNum() > 0) { // 当ShardingNumber大于0时表示分库分表
                // read sharding db info.
                DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
                if (dbClusterInfo == null) {
                    throw new DBClusterException("找不到相关的集群信息, clusterName=" + clusterName);
                }

                // 创建主库库表
                for (DBRegionInfo region : dbClusterInfo.getDbRegions()) {
                    for (DBInfo dbInfo : region.getMasterDBInfos()) {
                        Connection dbConn = dbInfo.getDatasource().getConnection();
                        int tableNum = table.getShardingNum();
                        this.dbGenerator.syncTable(dbConn, table, tableNum);
                        dbConn.close();
                    }
                }

                // 创建从库库表
                for (DBRegionInfo region : dbClusterInfo.getDbRegions()) {
                    List<List<DBInfo>> slaveDbs = region.getSlaveDBInfos();
                    for (List<DBInfo> slaveConns : slaveDbs) {
                        for (DBInfo dbConnInfo : slaveConns) {
                            Connection dbConn = dbConnInfo.getDatasource().getConnection();
                            int tableNum = table.getShardingNum();
                            this.dbGenerator.syncTable(dbConn, table, tableNum);
                            dbConn.close();
                        }
                    }
                }

            } else { // 当ShardingNumber等于0时表示全局表
                DBClusterInfo dbClusterInfo = this.dbClusterInfoC.find(clusterName);
                if (dbClusterInfo == null) {
                    throw new DBClusterException("加载集群失败，未知的集群，cluster name=" + clusterName);
                }
                // 全局主库
                DBInfo dbConnInfo = dbClusterInfo.getMasterGlobalDBInfo();
                if (dbConnInfo != null) {
                    DataSource globalDs = dbConnInfo.getDatasource();
                    if (globalDs != null) {
                        Connection conn = globalDs.getConnection();
                        this.dbGenerator.syncTable(conn, table);
                        conn.close();
                    }
                }

                // 全局从库
                List<DBInfo> slaveDbs = dbClusterInfo.getSlaveGlobalDBInfo();
                if (slaveDbs != null && !slaveDbs.isEmpty()) {
                    for (DBInfo slaveConnInfo : slaveDbs) {
                        Connection conn = slaveConnInfo.getDatasource().getConnection();
                        this.dbGenerator.syncTable(conn, table);
                        conn.close();
                    }
                }

            }

        }
    }

    private void _initDBCluster(Collection<DBClusterInfo> dbClusterInfos) throws LoadConfigException {
        this.dbClusterInfoC = DefaultContainerFactory.createContainer(ContainerType.MAP);

        for (DBClusterInfo dbClusterInfo : dbClusterInfos) {
            LOG.info("init db cluster " + dbClusterInfo.getClusterName() + ", router is ["
                    + dbClusterInfo.getRouterClass().getName() + "]");

            this.dbClusterInfoC.add(dbClusterInfo.getClusterName(), dbClusterInfo);

            // 初始化全局主库
            DBInfo masterGlobalDBInfo = dbClusterInfo.getMasterGlobalDBInfo();
            if (masterGlobalDBInfo != null) {
                buildDataSource(masterGlobalDBInfo);
                _initDatabaseName(masterGlobalDBInfo);
            }

            // 初始化全局从库
            List<DBInfo> slaveDbs = dbClusterInfo.getSlaveGlobalDBInfo();
            if (slaveDbs != null && !slaveDbs.isEmpty()) {
                for (DBInfo slaveGlobalDBInfo : slaveDbs) {
                    buildDataSource(slaveGlobalDBInfo);
                    _initDatabaseName(slaveGlobalDBInfo);
                }
            }

            // 初始化集群
            for (DBRegionInfo regionInfo : dbClusterInfo.getDbRegions()) {
                // 初始化集群主库
                for (DBInfo masterDBInfo : regionInfo.getMasterDBInfos()) {
                    buildDataSource(masterDBInfo);
                    _initDatabaseName(masterDBInfo);
                }

                // 初始化集群从库
                for (List<DBInfo> slaveConnections : regionInfo.getSlaveDBInfos()) {
                    for (DBInfo slaveDBInfo : slaveConnections) {
                        buildDataSource(slaveDBInfo);
                        _initDatabaseName(slaveDBInfo);
                    }
                }
            }

        }
    }

    private void _initDatabaseName(DBInfo dbInfo) {
        DataSource ds = dbInfo.getDatasource();

        if (ds != null) {
            Connection conn = null;
            try {
                conn = ds.getConnection();
                String dbName = conn.getCatalog();
                dbInfo.setDbName(dbName);
            } catch (Exception e) {
                throw new RuntimeException("get database name failure ", e);
            } finally {
                JdbcUtil.close(conn);
            }
        }

    }

    /**
     * 读取配置.
     * 
     * @return 配置信息.
     */
    private IClusterConfig _getConfig(String xmlFilePath) throws LoadConfigException {
        IClusterConfig config = null;

        if (StringUtils.isBlank(xmlFilePath)) {
            config = XmlClusterConfigImpl.getInstance();
        } else {
            config = XmlClusterConfigImpl.getInstance(xmlFilePath);
        }

        return config;
    }

    /**
     * 创建数据源连接.
     */
    public abstract void buildDataSource(DBInfo dbInfo) throws LoadConfigException;

    /**
     * 关闭数据源连接
     * 
     * @param dbConnInfo
     */
    public abstract void closeDataSource(DBInfo dbConnInfo);

    public EnumSyncAction getSyncAction() {
        return syncAction;
    }

    @Override
    public void setSyncAction(EnumSyncAction syncAction) {
        this.syncAction = syncAction;
    }

    @Override
    public IIdGenerator getIdGenerator() {
        return this.idGenerator;
    }

    @Override
    public void setScanPackage(String scanPackage) {
        this.scanPackage = scanPackage;
    }

    @Override
    public ITableCluster getTableCluster() {
        return this.tableCluster;
    }

}
