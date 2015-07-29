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

package org.pinus4j.cluster.config.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cache.beans.PrimaryCacheInfo;
import org.pinus4j.cache.beans.SecondCacheInfo;
import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.DBRegionInfo;
import org.pinus4j.cluster.beans.DBRegionInfo.Value;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.HashAlgoEnum;
import org.pinus4j.cluster.router.IClusterRouter;
import org.pinus4j.constant.Const;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtils;
import org.pinus4j.utils.XmlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import com.google.common.collect.Maps;

/**
 * xml config implements.
 *
 * @author duanbn
 * @since 0.1
 */
public class XmlClusterConfigImpl implements IClusterConfig {

    public static final Logger               LOG                        = LoggerFactory
                                                                                .getLogger(XmlClusterConfigImpl.class);

    /**
     * 主键批量生成数
     */
    private static int                       idGenerateBatch;

    /**
     * hash算法.
     */
    private static HashAlgoEnum              hashAlgo;

    private static String                    defaultConnectionPoolClass = DEFAULT_CP_CLASS;

    /**
     * cache config param.
     */
    private static boolean                   isCacheEnabled;
    private static PrimaryCacheInfo          primaryCacheInfo;
    private static SecondCacheInfo           secondCacheInfo;

    /**
     * DB集群信息.
     */
    private static Collection<DBClusterInfo> dbClusterInfos             = new ArrayList<DBClusterInfo>();

    private XmlUtil                          xmlUtil;

    /**
     * zookeeper连接地址.
     */
    private static String                    zkUrl;

    private static Map<String, DBInfo>       dsBucketMap                = Maps.newHashMap();

    private XmlClusterConfigImpl() throws LoadConfigException {
        this(null);
    }

    private XmlClusterConfigImpl(String xmlFilePath) throws LoadConfigException {
        if (StringUtils.isBlank(xmlFilePath))
            xmlUtil = XmlUtil.getInstance();
        else
            xmlUtil = XmlUtil.getInstance(new File(xmlFilePath));

        Node root = xmlUtil.getRoot();
        if (root == null) {
            throw new LoadConfigException("can not found root node");
        }

        try {
            // load id generator
            _loadIdGeneratorBatch(root);

            // load zookeeper url
            _loadZkUrl(root);

            // load hash algo
            _loadHashAlgo(root);

            // load datasource bucket
            _loadDatasourceBucket(root);

            // load cluster info
            _loadDBClusterInfo(root);

            // load cache info
            _loadCacheInfo(root);
        } catch (Exception e) {
            throw new LoadConfigException(e);
        }

    }

    private void _loadDatasourceBucket(Node root) throws LoadConfigException {
        Node dsBucket = xmlUtil.getFirstChildByName(root, "datasource-bucket");
        if (dsBucket == null) {
            throw new LoadConfigException("can not found <datasource-bucket>");
        }

        String cpClassFullpath = xmlUtil.getAttributeValue(dsBucket, "cpclass");
        if (StringUtils.isNotBlank(cpClassFullpath)) {
            defaultConnectionPoolClass = cpClassFullpath;
        }

        String dbInfoId = null;
        DBInfo dbInfo = null;

        for (Node appDBInfoNode : xmlUtil.getChildByName(dsBucket, "appds")) {
            dbInfoId = xmlUtil.getAttributeValue(appDBInfoNode, "id");
            if (StringUtils.isBlank(dbInfoId))
                throw new LoadConfigException("<envds> <appds> 必须设置id属性");
            dbInfo = new AppDBInfo();
            String username = xmlUtil.getFirstChildByName(appDBInfoNode, "username").getTextContent().trim();
            String password = xmlUtil.getFirstChildByName(appDBInfoNode, "password").getTextContent().trim();
            String url = xmlUtil.getFirstChildByName(appDBInfoNode, "url").getTextContent().trim();
            ((AppDBInfo) dbInfo).setUsername(username);
            ((AppDBInfo) dbInfo).setPassword(password);
            ((AppDBInfo) dbInfo).setUrl(url);
            Map<String, String> connectParam = xmlUtil.getAttributeAsMap(dsBucket, "cpclass");
            Map<String, String> oneDBConnectParam = xmlUtil.getAttributeAsMap(appDBInfoNode, "id");
            for (Map.Entry<String, String> connectParamEntry : oneDBConnectParam.entrySet()) {
                connectParam.put(connectParamEntry.getKey(), connectParamEntry.getValue());
            }
            ((AppDBInfo) dbInfo).setConnPoolInfo(connectParam);

            if (dsBucketMap.get(dbInfoId) != null) {
                throw new LoadConfigException("配置错误，数据源id配置重复，id=" + dbInfoId);
            }
            dsBucketMap.put(dbInfoId, dbInfo);
        }

        for (Node envDBInfoNode : xmlUtil.getChildByName(dsBucket, "envds")) {
            dbInfo = new EnvDBInfo();
            String envDsName = xmlUtil.getFirstChildByName(envDBInfoNode, "jndi").getTextContent().trim();
            ((EnvDBInfo) dbInfo).setEnvDsName(envDsName);

            if (dsBucketMap.get(dbInfoId) != null) {
                throw new LoadConfigException("配置错误，数据源id配置重复，id=" + dbInfoId);
            }
            dsBucketMap.put(dbInfoId, dbInfo);
        }

    }

    /**
     * load db.cluster.generateid.batch.
     */
    private void _loadIdGeneratorBatch(Node root) throws LoadConfigException {
        Node idGeneratorBatchNode = xmlUtil.getFirstChildByName(root, Const.PROP_IDGEN_BATCH);
        try {
            idGenerateBatch = Integer.parseInt(idGeneratorBatchNode.getTextContent().trim());
        } catch (NumberFormatException e) {
            throw new LoadConfigException(e);
        }
    }

    /**
     * load db.cluster.zk.
     */
    private void _loadZkUrl(Node root) throws LoadConfigException {
        Node zkUrlNode = xmlUtil.getFirstChildByName(root, Const.PROP_ZK_URL);
        zkUrl = zkUrlNode.getTextContent().trim();
    }

    /**
     * load db.cluster.hash.algo.
     */
    private void _loadHashAlgo(Node root) throws LoadConfigException {
        Node hashAlgoNode = xmlUtil.getFirstChildByName(root, Const.PROP_HASH_ALGO);
        hashAlgo = HashAlgoEnum.getEnum(hashAlgoNode.getTextContent().trim());
    }

    private List<Value> _parseCapacity(String clusterName, String regionCapacity) throws LoadConfigException {
        List<Value> values = new ArrayList<Value>();

        String[] aa = regionCapacity.split("\\,");
        Value value = null;
        for (String bb : aa) {
            String[] cc = bb.split("\\-");
            if (cc.length != 2) {
                throw new LoadConfigException("解析集群容量错误");
            }

            long start = -1, end = -1;
            try {
                start = Long.parseLong(cc[0]);
                end = Long.parseLong(cc[1]);
            } catch (Exception e) {
                throw new LoadConfigException("解析集群容量错误, clusterName=" + clusterName, e);
            }

            if (start < 0 || end < 0 || end <= start) {
                throw new LoadConfigException("集群容量参数有误, clusterName=" + clusterName + ", start=" + start + ", end="
                        + end);
            }

            value = new Value();
            value.start = start;
            value.end = end;

            values.add(value);
        }

        return values;
    }

    @SuppressWarnings("unchecked")
    private DBClusterInfo _getDBClusterInfo(String clusterName, Node clusterNode) throws LoadConfigException {
        DBClusterInfo dbClusterInfo = new DBClusterInfo();
        // set cluster name
        dbClusterInfo.setClusterName(clusterName);

        // set router class
        try {
            String classFullPath = xmlUtil.getAttributeValue(clusterNode, "router");
            if (StringUtils.isBlank(classFullPath)) {
                classFullPath = DEFAULT_CLUSTER_ROUTER_CLASS;
            }
            Class<IClusterRouter> clazz = (Class<IClusterRouter>) Class.forName(classFullPath);
            dbClusterInfo.setRouterClass(clazz);
        } catch (Exception e) {
            throw new LoadConfigException(e);
        }

        //
        // load global
        //
        Node global = xmlUtil.getFirstChildByName(clusterNode, "global");
        if (global != null) {
            // load master global
            Node masterGlobal = xmlUtil.getFirstChildByName(global, "master");
            String dsId = masterGlobal.getTextContent().trim();
            DBInfo masterGlobalDBInfo = dsBucketMap.get(dsId);
            if (masterGlobalDBInfo == null) {
                throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
            }
            masterGlobalDBInfo.setClusterName(clusterName);
            masterGlobalDBInfo.setMasterSlave(EnumDBMasterSlave.MASTER);
            // set custom property
            Map<String, String> propMap = xmlUtil.getAttributeAsMap(masterGlobal);
            masterGlobalDBInfo.setCustomProperties(propMap);
            masterGlobalDBInfo.check();

            dbClusterInfo.setMasterGlobalDBInfo(masterGlobalDBInfo);

            // load slave global
            List<Node> slaveGlobalList = xmlUtil.getChildByName(global, "slave");
            if (slaveGlobalList != null && !slaveGlobalList.isEmpty()) {
                List<DBInfo> slaveGlobalConnection = new ArrayList<DBInfo>();

                int slaveIndex = 0;
                for (Node slaveGlobal : slaveGlobalList) {
                    dsId = slaveGlobal.getTextContent().trim();
                    DBInfo slaveGlobalDBInfo = dsBucketMap.get(dsId);
                    if (slaveGlobalDBInfo == null) {
                        throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
                    }
                    slaveGlobalDBInfo.setClusterName(clusterName);
                    slaveGlobalDBInfo.setMasterSlave(EnumDBMasterSlave.getSlaveEnum(slaveIndex++));
                    // set custom property
                    propMap = xmlUtil.getAttributeAsMap(slaveGlobal);
                    slaveGlobalDBInfo.setCustomProperties(propMap);
                    slaveGlobalDBInfo.check();
                    slaveGlobalConnection.add(slaveGlobalDBInfo);
                }

                dbClusterInfo.setSlaveGlobalDBInfo(slaveGlobalConnection);
            }
        }

        //
        // load region
        //
        List<DBRegionInfo> dbRegions = new ArrayList<DBRegionInfo>();
        List<Node> regionNodeList = xmlUtil.getChildByName(clusterNode, "region");
        DBRegionInfo regionInfo = null;
        for (Node regionNode : regionNodeList) {
            regionInfo = new DBRegionInfo();

            // load cluster capacity
            String regionCapacity = xmlUtil.getAttributeValue(regionNode, "capacity");
            if (regionCapacity == null) {
                throw new LoadConfigException("<region>需要配置capacity属性");
            }
            List<Value> values = _parseCapacity(clusterName, regionCapacity);
            regionInfo.setValues(values);
            regionInfo.setCapacity(regionCapacity);

            // load region master
            List<DBInfo> regionMasterConnection = new ArrayList<DBInfo>();
            Node master = xmlUtil.getFirstChildByName(regionNode, "master");
            List<Node> shardingNodeList = xmlUtil.getChildByName(master, "sharding");
            for (Node shardingNode : shardingNodeList) {
                String dsId = shardingNode.getTextContent().trim();
                DBInfo masterShardingDBInfo = dsBucketMap.get(dsId);
                if (masterShardingDBInfo == null) {
                    throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
                }
                masterShardingDBInfo.setClusterName(clusterName);
                masterShardingDBInfo.setMasterSlave(EnumDBMasterSlave.MASTER);
                // set custom property
                Map<String, String> propMap = xmlUtil.getAttributeAsMap(shardingNode);
                masterShardingDBInfo.setCustomProperties(propMap);
                masterShardingDBInfo.check();
                regionMasterConnection.add(masterShardingDBInfo);
            }
            regionInfo.setMasterDBInfos(regionMasterConnection);

            // load region slave
            List<List<DBInfo>> regionSlaveConnection = new ArrayList<List<DBInfo>>();
            List<Node> slaveNodeList = xmlUtil.getChildByName(regionNode, "slave");
            int slaveIndex = 0;
            for (Node slaveNode : slaveNodeList) {
                shardingNodeList = xmlUtil.getChildByName(slaveNode, "sharding");

                List<DBInfo> slaveConnections = new ArrayList<DBInfo>();
                for (Node shardingNode : shardingNodeList) {
                    String dsId = shardingNode.getTextContent().trim();
                    DBInfo slaveShardingDBInfo = dsBucketMap.get(dsId);
                    if (slaveShardingDBInfo == null) {
                        throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
                    }
                    slaveShardingDBInfo.setClusterName(clusterName);
                    slaveShardingDBInfo.setMasterSlave(EnumDBMasterSlave.getSlaveEnum(slaveIndex++));
                    // set custom property
                    Map<String, String> propMap = xmlUtil.getAttributeAsMap(shardingNode);
                    slaveShardingDBInfo.setCustomProperties(propMap);
                    slaveShardingDBInfo.check();
                    slaveConnections.add(slaveShardingDBInfo);
                }

                regionSlaveConnection.add(slaveConnections);
            }
            regionInfo.setSlaveDBInfos(regionSlaveConnection);

            dbRegions.add(regionInfo);
        }
        dbClusterInfo.setDbRegions(dbRegions);

        return dbClusterInfo;
    }

    private void _loadDBClusterInfo(Node root) throws LoadConfigException {
        List<Node> clusterNodeList = xmlUtil.getChildByName(root, "cluster");

        for (Node clusterNode : clusterNodeList) {
            String name = xmlUtil.getAttributeValue(clusterNode, "name");
            dbClusterInfos.add(_getDBClusterInfo(name, clusterNode));
        }
    }

    @SuppressWarnings("unchecked")
    private void _loadCacheInfo(Node root) throws LoadConfigException {
        Node dbClusterCacheNode = xmlUtil.getFirstChildByName(root, Const.PROP_DB_CLUSTER_CACHE);
        if (dbClusterCacheNode == null) {
            throw new LoadConfigException("can not found node " + Const.PROP_DB_CLUSTER_CACHE);
        }

        try {
            String isCacheEnabled = xmlUtil.getAttributeValue(dbClusterCacheNode, "enabled");
            if (StringUtils.isNotBlank(isCacheEnabled)) {
                XmlClusterConfigImpl.isCacheEnabled = Boolean.valueOf(isCacheEnabled);
            }

            if (XmlClusterConfigImpl.isCacheEnabled) {
                Node primaryNode = xmlUtil.getFirstChildByName(dbClusterCacheNode, Const.PROP_DB_CLUSTER_CACHE_PRIMARY);
                int primaryCacheExpire = Integer.parseInt(xmlUtil.getAttributeValue(primaryNode, "expire"));
                String classFullPath = xmlUtil.getAttributeValue(primaryNode, "class");
                if (StringUtils.isBlank(classFullPath)) {
                    classFullPath = DEFAULT_PRIMARY_CACHE_CLASS;
                }
                Class<IPrimaryCache> primaryCacheClass = (Class<IPrimaryCache>) Class.forName(classFullPath);
                Node primaryAddressNode = xmlUtil.getFirstChildByName(primaryNode, Const.PROP_DB_CLUSTER_CACHE_ADDRESS);
                String primaryCacheAddress = primaryAddressNode.getTextContent().trim();
                primaryCacheInfo = new PrimaryCacheInfo();
                primaryCacheInfo.setPrimaryCacheAddress(primaryCacheAddress);
                primaryCacheInfo.setPrimaryCacheClass(primaryCacheClass);
                primaryCacheInfo.setPrimaryCacheExpire(primaryCacheExpire);
                Map<String, String> attrMap = xmlUtil.getAttributeAsMap(primaryNode, "expire", "class");
                primaryCacheInfo.setPrimaryCacheAttr(attrMap);

                Node secondNode = xmlUtil.getFirstChildByName(dbClusterCacheNode, Const.PROP_DB_CLUSTER_CACHE_SECOND);
                int secondCacheExpire = Integer.parseInt(xmlUtil.getAttributeValue(secondNode, "expire"));
                classFullPath = xmlUtil.getAttributeValue(secondNode, "class");
                if (StringUtils.isBlank(classFullPath)) {
                    classFullPath = DEFAULT_SECOND_CACHE_CLASS;
                }
                Class<ISecondCache> secondCacheClass = (Class<ISecondCache>) Class.forName(classFullPath);
                Node secondAddressNode = xmlUtil.getFirstChildByName(secondNode, Const.PROP_DB_CLUSTER_CACHE_ADDRESS);
                String secondCacheAddress = secondAddressNode.getTextContent().trim();
                secondCacheInfo = new SecondCacheInfo();
                secondCacheInfo.setSecondCacheAddress(secondCacheAddress);
                secondCacheInfo.setSecondCacheClass(secondCacheClass);
                secondCacheInfo.setSecondCacheExpire(secondCacheExpire);
                attrMap = xmlUtil.getAttributeAsMap(secondNode, "expire", "class");
                secondCacheInfo.setSecondCacheAttr(attrMap);
            }
        } catch (Exception e) {
            throw new LoadConfigException("parse db.cluster.cache failure", e);
        }
    }

    private static IClusterConfig instance;

    public static IClusterConfig getInstance() throws LoadConfigException {
        if (instance == null) {
            synchronized (XmlClusterConfigImpl.class) {
                if (instance == null) {
                    instance = new XmlClusterConfigImpl();
                }
            }
        }

        return instance;
    }

    public static IClusterConfig getInstance(String xmlFilePath) throws LoadConfigException {
        if (instance == null) {
            synchronized (XmlClusterConfigImpl.class) {
                if (instance == null) {
                    instance = new XmlClusterConfigImpl(xmlFilePath);
                }
            }
        }

        return instance;
    }

    @Override
    public int getIdGeneratorBatch() {
        return idGenerateBatch;
    }

    @Override
    public HashAlgoEnum getHashAlgo() {
        return hashAlgo;
    }

    @Override
    public Collection<DBClusterInfo> getDBClusterInfos() {
        return dbClusterInfos;
    }

    @Override
    public String getZookeeperUrl() {
        return zkUrl;
    }

    @Override
    public IDBConnectionPool getImplConnectionPool() {
        try {
            Class<?> clazz = Class.forName(defaultConnectionPoolClass);

            return (IDBConnectionPool) clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isCacheEnabled() {
        return isCacheEnabled;
    }

    @Override
    public PrimaryCacheInfo getPrimaryCacheInfo() {
        return primaryCacheInfo;
    }

    @Override
    public SecondCacheInfo getSecondCacheInfo() {
        return secondCacheInfo;
    }

}
