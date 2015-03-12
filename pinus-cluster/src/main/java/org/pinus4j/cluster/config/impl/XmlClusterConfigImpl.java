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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.DBRegionInfo;
import org.pinus4j.cluster.beans.DBRegionInfo.Value;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.enums.EnumClusterCatalog;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumDbConnectionPoolCatalog;
import org.pinus4j.cluster.enums.HashAlgoEnum;
import org.pinus4j.cluster.router.IClusterRouter;
import org.pinus4j.constant.Const;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtils;
import org.pinus4j.utils.XmlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * xml config implements.
 *
 * @author duanbn
 * @since 0.1
 */
public class XmlClusterConfigImpl implements IClusterConfig {

	public static final Logger LOG = LoggerFactory.getLogger(XmlClusterConfigImpl.class);

	/**
	 * 数据库连接方式. 从应用加载连接，或者从容器加载连接.
	 */
	private static EnumDbConnectionPoolCatalog enumCpCatalog;

	/**
	 * 主键批量生成数
	 */
	private static int idGenerateBatch;

	/**
	 * hash算法.
	 */
	private static HashAlgoEnum hashAlgo;

	/**
	 * cache config param.
	 */
	private static boolean isCacheEnabled;
	private static Class<IPrimaryCache> primaryCacheClass;
	private static int primaryCacheExpire;
	private static String primaryCacheAddress;

	private static Class<ISecondCache> secondCacheClass;
	private static int secondCacheExpire;
	private static String secondCacheAddress;

	/**
	 * DB集群信息.
	 */
	private static Collection<DBClusterInfo> dbClusterInfos = new ArrayList<DBClusterInfo>();

	private XmlUtil xmlUtil;

	/**
	 * zookeeper连接地址.
	 */
	private static String zkUrl;

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

			// load cluster info
			_loadDBClusterInfo(root);

			// load cache info
			_loadCacheInfo(root);
		} catch (Exception e) {
			throw new LoadConfigException(e);
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

	/**
	 * load db-connection-pool
	 */
	private Map<String, Object> _loadDbConnectInfo(Node connPoolNode) throws LoadConfigException {
		Map<String, Object> map = new HashMap<String, Object>();

		NodeList child = connPoolNode.getChildNodes();
		Node currentNode = null;
		for (int i = 0; i < child.getLength(); i++) {
			currentNode = child.item(i);
			if (currentNode.getNodeName().equals(Const.PROP_MAXACTIVE)) {
				map.put(Const.PROP_MAXACTIVE, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_MINIDLE)) {
				map.put(Const.PROP_MINIDLE, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_MAXIDLE)) {
				map.put(Const.PROP_MAXIDLE, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_INITIALSIZE)) {
				map.put(Const.PROP_INITIALSIZE, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_REMOVEABANDONED)) {
				map.put(Const.PROP_REMOVEABANDONED, Boolean.valueOf(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_REMOVEABANDONEDTIMEOUT)) {
				map.put(Const.PROP_REMOVEABANDONEDTIMEOUT, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_MAXWAIT)) {
				map.put(Const.PROP_MAXWAIT, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_TIMEBETWEENEVICTIONRUNSMILLIS)) {
				map.put(Const.PROP_TIMEBETWEENEVICTIONRUNSMILLIS, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_NUMTESTSPEREVICTIONRUN)) {
				map.put(Const.PROP_NUMTESTSPEREVICTIONRUN, Integer.parseInt(currentNode.getTextContent().trim()));
			}
			if (currentNode.getNodeName().equals(Const.PROP_MINEVICTABLEIDLETIMEMILLIS)) {
				map.put(Const.PROP_MINEVICTABLEIDLETIMEMILLIS, Integer.parseInt(currentNode.getTextContent().trim()));
			}
		}
		return map;
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

	/**
	 * load db info.
	 */
	private DBInfo _getDBInfo(String clusterName, Node node, EnumDBMasterSlave masterSlave) throws LoadConfigException {
		DBInfo dbInfo = null;

		Node root = xmlUtil.getRoot();
		if (root == null) {
			throw new LoadConfigException("找不到root节点");
		}

		Node connPoolNode = xmlUtil.getFirstChildByName(root, "db-connection-pool");
		if (connPoolNode == null) {
			throw new LoadConfigException("找不到db-connection-pool节点");
		}

		Node cpCatalogNode = connPoolNode.getAttributes().getNamedItem("catalog");
		String cpCatalog = EnumDbConnectionPoolCatalog.APP.getValue();
		if (cpCatalogNode != null) {
			cpCatalog = cpCatalogNode.getTextContent();
		}
		enumCpCatalog = EnumDbConnectionPoolCatalog.getEnum(cpCatalog);

		switch (enumCpCatalog) {
		case ENV:
			dbInfo = new EnvDBInfo();
			dbInfo.setClusterName(clusterName);
			dbInfo.setMasterSlave(masterSlave);

			String envDsName = node.getTextContent().trim();
			((EnvDBInfo) dbInfo).setEnvDsName(envDsName);
			break;
		case APP:
			dbInfo = new AppDBInfo();
			dbInfo.setClusterName(clusterName);
			dbInfo.setMasterSlave(masterSlave);

			String username = xmlUtil.getFirstChildByName(node, "db.username").getTextContent().trim();
			String password = xmlUtil.getFirstChildByName(node, "db.password").getTextContent().trim();
			String url = xmlUtil.getFirstChildByName(node, "db.url").getTextContent().trim();
			((AppDBInfo) dbInfo).setUsername(username);
			((AppDBInfo) dbInfo).setPassword(password);
			((AppDBInfo) dbInfo).setUrl(url);
			((AppDBInfo) dbInfo).setConnPoolInfo(_loadDbConnectInfo(connPoolNode));
			break;
		default:
			throw new LoadConfigException("catalog attribute of db-connection-pool config error, catalog = "
					+ cpCatalog + " you should be select in \"env\" or \"app\"");
		}

		// set custom property
		NamedNodeMap attrMap = node.getAttributes();
		Map<String, String> propMap = new HashMap<String, String>(attrMap.getLength());
		Node attr = null;
		for (int i = 0; i < attrMap.getLength(); i++) {
			attr = attrMap.item(i);
			propMap.put(attr.getNodeName(), attr.getNodeValue());
		}
		dbInfo.setCustomProperties(propMap);

		dbInfo.check();

		return dbInfo;
	}

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

		// set cluster catalog
		String catalog = xmlUtil.getAttributeValue(clusterNode, "catalog");
		dbClusterInfo.setCatalog(EnumClusterCatalog.getEnum(catalog));

		//
		// load global
		//
		Node global = xmlUtil.getFirstChildByName(clusterNode, "global");
		if (global != null) {
			// load master global
			Node masterGlobal = xmlUtil.getFirstChildByName(global, "master");
			DBInfo masterGlobalConnection = _getDBInfo(clusterName, masterGlobal, EnumDBMasterSlave.MASTER);
			dbClusterInfo.setMasterGlobalDBInfo(masterGlobalConnection);

			// load slave global
			List<Node> slaveGlobalList = xmlUtil.getChildByName(global, "slave");
			if (slaveGlobalList != null && !slaveGlobalList.isEmpty()) {
				List<DBInfo> slaveGlobalConnection = new ArrayList<DBInfo>();

				int slaveIndex = 0;
				for (Node slaveGlobal : slaveGlobalList) {
					slaveGlobalConnection.add(_getDBInfo(clusterName, slaveGlobal,
							EnumDBMasterSlave.getSlaveEnum(slaveIndex++)));
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
				regionMasterConnection.add(_getDBInfo(clusterName, shardingNode, EnumDBMasterSlave.MASTER));
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
					slaveConnections.add(_getDBInfo(clusterName, shardingNode,
							EnumDBMasterSlave.getSlaveEnum(slaveIndex)));
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
			String catalog = xmlUtil.getAttributeValue(clusterNode, "catalog");
			if (!catalog.equalsIgnoreCase(EnumClusterCatalog.MYSQL.getValue())) {
				continue;
			}

			String name = xmlUtil.getAttributeValue(clusterNode, "name");
			dbClusterInfos.add(_getDBClusterInfo(name, clusterNode));
		}
	}

	private void _loadCacheInfo(Node root) throws LoadConfigException {
		Node dbClusterCacheNode = xmlUtil.getFirstChildByName(root, Const.PROP_DB_CLUSTER_CACHE);
		if (dbClusterCacheNode == null) {
			throw new LoadConfigException("can not found node " + Const.PROP_DB_CLUSTER_CACHE);
		}

		try {
			String isCacheEnabled = xmlUtil.getAttributeValue(dbClusterCacheNode, "enabled");
			if (StringUtils.isNotBlank(isCacheEnabled)) {
				this.isCacheEnabled = Boolean.valueOf(isCacheEnabled);
			}

			if (this.isCacheEnabled) {
				Node primaryNode = xmlUtil.getFirstChildByName(dbClusterCacheNode, Const.PROP_DB_CLUSTER_CACHE_PRIMARY);
				primaryCacheExpire = Integer.parseInt(xmlUtil.getAttributeValue(primaryNode, "expire"));
				String classFullPath = xmlUtil.getAttributeValue(primaryNode, "class");
				if (StringUtils.isBlank(classFullPath)) {
					classFullPath = DEFAULT_PRIMARY_CACHE_CLASS;
				}
				primaryCacheClass = (Class<IPrimaryCache>) Class.forName(classFullPath);
				Node primaryAddressNode = xmlUtil.getFirstChildByName(primaryNode, Const.PROP_DB_CLUSTER_CACHE_ADDRESS);
				primaryCacheAddress = primaryAddressNode.getTextContent().trim();

				Node secondNode = xmlUtil.getFirstChildByName(dbClusterCacheNode, Const.PROP_DB_CLUSTER_CACHE_SECOND);
				secondCacheExpire = Integer.parseInt(xmlUtil.getAttributeValue(secondNode, "expire"));
				classFullPath = xmlUtil.getAttributeValue(secondNode, "class");
				if (StringUtils.isBlank(classFullPath)) {
					classFullPath = DEFAULT_SECOND_CACHE_CLASS;
				}
				secondCacheClass = (Class<ISecondCache>) Class.forName(classFullPath);
				Node secondAddressNode = xmlUtil.getFirstChildByName(secondNode, Const.PROP_DB_CLUSTER_CACHE_ADDRESS);
				secondCacheAddress = secondAddressNode.getTextContent().trim();
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
	public EnumDbConnectionPoolCatalog getDbConnectionPoolCatalog() {
		return enumCpCatalog;
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
	public boolean isCacheEnabled() {
		return this.isCacheEnabled;
	}

	@Override
	public String getSecondCacheAddress() {
		return secondCacheAddress;
	}

	@Override
	public int getSecondCacheExpire() {
		return secondCacheExpire;
	}

	@Override
	public Class<ISecondCache> getSecondCacheClass() {
		return secondCacheClass;
	}

	@Override
	public String getPrimaryCacheAddress() {
		return primaryCacheAddress;
	}

	@Override
	public int getPrimaryCacheExpire() {
		return primaryCacheExpire;
	}

	@Override
	public Class<IPrimaryCache> getPrimaryCacheClass() {
		return primaryCacheClass;
	}

}
