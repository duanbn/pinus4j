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

import org.pinus4j.cache.beans.PrimaryCacheInfo;
import org.pinus4j.cache.beans.SecondCacheInfo;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.config.loader.IXMLConfigLoader;
import org.pinus4j.cluster.config.loader.impl.CacheEnabledLoader;
import org.pinus4j.cluster.config.loader.impl.ConnectionPoolClassLoader;
import org.pinus4j.cluster.config.loader.impl.DBClusterInfoLoader;
import org.pinus4j.cluster.config.loader.impl.DataSourceBucketLoader;
import org.pinus4j.cluster.config.loader.impl.PrimaryCacheInfoLoader;
import org.pinus4j.cluster.config.loader.impl.SecondCacheInfoLoader;
import org.pinus4j.cluster.container.IContainer;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.cluster.enums.HashAlgoEnum;
import org.pinus4j.constant.Const;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtil;
import org.pinus4j.utils.XmlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

/**
 * xml config implements.
 *
 * @author duanbn
 * @since 0.1
 */
public class XMLClusterConfigImpl implements IClusterConfig {

    public static final Logger               LOG            = LoggerFactory.getLogger(XMLClusterConfigImpl.class);

    /**
     * 主键批量生成数
     */
    private static int                       idGenerateBatch;

    /**
     * hash算法.
     */
    private static HashAlgoEnum              hashAlgo;

    /**
     * cache config param.
     */
    private static boolean                   isCacheEnabled;
    private static PrimaryCacheInfo          primaryCacheInfo;
    private static SecondCacheInfo           secondCacheInfo;

    /**
     * DB集群信息.
     */
    private static Collection<DBClusterInfo> dbClusterInfos = new ArrayList<DBClusterInfo>();

    private XmlUtil                          xmlUtil;

    /**
     * zookeeper连接地址.
     */
    private static String                    zkUrl;

    private XMLClusterConfigImpl() throws LoadConfigException {
        this(null);
    }

    private XMLClusterConfigImpl(String xmlFilePath) throws LoadConfigException {
        if (StringUtil.isBlank(xmlFilePath))
            xmlUtil = XmlUtil.getInstance();
        else
            xmlUtil = XmlUtil.getInstance(new File(xmlFilePath));

        Node root = xmlUtil.getRoot();
        if (root == null) {
            throw new LoadConfigException("can not found root node");
        }

        // load id generator
        _loadIdGeneratorBatch(root);

        // load zookeeper url
        _loadZkUrl(root);

        // load hash algo
        _loadHashAlgo(root);

        // load datasource bucket
        IXMLConfigLoader<IContainer<DBInfo>> dbInfoLoader = new DataSourceBucketLoader();
        IContainer<DBInfo> dbInfos = dbInfoLoader.load(root);

        // load cluster info
        IXMLConfigLoader<List<DBClusterInfo>> dbClusterInfoLoader = new DBClusterInfoLoader(dbInfos);
        dbClusterInfos.addAll(dbClusterInfoLoader.load(root));

        // load cache info
        IXMLConfigLoader<Boolean> cacheEnabledLoader = new CacheEnabledLoader();
        isCacheEnabled = cacheEnabledLoader.load(root);
        if (isCacheEnabled) {
            IXMLConfigLoader<PrimaryCacheInfo> primaryCacheInfoLoader = new PrimaryCacheInfoLoader();
            primaryCacheInfo = primaryCacheInfoLoader.load(root);

            IXMLConfigLoader<SecondCacheInfo> secondCacheInfoLoader = new SecondCacheInfoLoader();
            secondCacheInfo = secondCacheInfoLoader.load(root);
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

    private static volatile IClusterConfig instance;

    public static IClusterConfig getInstance() throws LoadConfigException {
        if (instance == null) {
            synchronized (XMLClusterConfigImpl.class) {
                if (instance == null) {
                    instance = new XMLClusterConfigImpl();
                }
            }
        }

        return instance;
    }

    public static IClusterConfig getInstance(String xmlFilePath) throws LoadConfigException {
        if (instance == null) {
            synchronized (XMLClusterConfigImpl.class) {
                if (instance == null) {
                    instance = new XMLClusterConfigImpl(xmlFilePath);
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
            ConnectionPoolClassLoader loader = new ConnectionPoolClassLoader();
            String connectionPoolClass = loader.load(xmlUtil.getRoot());

            Class<?> clazz = Class.forName(connectionPoolClass);

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
