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

package com.pinus.config.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.api.enums.EnumDbConnectionPoolCatalog;
import com.pinus.cluster.beans.AppDBConnectionInfo;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBClusterRegionInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.beans.EnvDBConnectionInfo;
import com.pinus.cluster.enums.EnumClusterCatalog;
import com.pinus.cluster.enums.HashAlgoEnum;
import com.pinus.config.IClusterConfig;
import com.pinus.constant.Const;
import com.pinus.exception.LoadConfigException;
import com.pinus.util.StringUtils;
import com.pinus.util.XmlUtil;

public class XmlDBClusterConfigImpl implements IClusterConfig, Watcher {

	public static final Logger LOG = Logger.getLogger(XmlDBClusterConfigImpl.class);

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
	 * DB集群信息.
	 */
	private static Map<String, DBClusterInfo> dbClusterInfo = new HashMap<String, DBClusterInfo>();

	private XmlUtil xmlUtil;

	/**
	 * zookeeper连接地址.
	 */
	private static String zkUrl;
	private CountDownLatch connectedLatch = new CountDownLatch(1);
	private int sessionTimeout = 30000;

	private XmlDBClusterConfigImpl() throws LoadConfigException {
		this(null);
	}

	private XmlDBClusterConfigImpl(String xmlFilePath) throws LoadConfigException {
		if (StringUtils.isBlank(xmlFilePath))
			xmlUtil = XmlUtil.getInstance();
		else
			xmlUtil = XmlUtil.getInstance(new File(xmlFilePath));

		try {
			// 加载id生成器
			_loadIdGeneratorBatch();

			// 加载zookeeper
			_loadZkUrl();

			// 加载hash algo
			_loadHashAlgo();

			_loadDBClusterInfo();
		} catch (Exception e) {
			throw new LoadConfigException(e);
		}

	}

	/**
	 * zookeeper节点的监视器
	 */
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedLatch.countDown();
		}
	}

	@Override
	public ZooKeeper getZooKeeper() {
		// 创建zookeeper连接
		try {
			ZooKeeper zk = new ZooKeeper(this.zkUrl, sessionTimeout, this);
			if (States.CONNECTING == zk.getState()) {
				connectedLatch.await();
			}

			return zk;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void _loadIdGeneratorBatch() throws LoadConfigException {
		Node root = xmlUtil.getRoot();
		if (root == null) {
			throw new LoadConfigException("找不到root节点");
		}

		Node idGeneratorBatchNode = xmlUtil.getFirstChildByName(root, Const.PROP_IDGEN_BATCH);
		try {
			idGenerateBatch = Integer.parseInt(idGeneratorBatchNode.getTextContent().trim());
		} catch (NumberFormatException e) {
			throw new LoadConfigException(e);
		}
	}

	private void _loadZkUrl() throws LoadConfigException {
		Node root = xmlUtil.getRoot();
		if (root == null) {
			throw new LoadConfigException("找不到root节点");
		}

		Node zkUrlNode = xmlUtil.getFirstChildByName(root, Const.PROP_ZK_URL);
		zkUrl = zkUrlNode.getTextContent().trim();
	}

	private void _loadHashAlgo() throws LoadConfigException {
		Node root = xmlUtil.getRoot();
		if (root == null) {
			throw new LoadConfigException("找不到root节点");
		}

		Node hashAlgoNode = xmlUtil.getFirstChildByName(root, Const.PROP_HASH_ALGO);
		hashAlgo = HashAlgoEnum.getEnum(hashAlgoNode.getTextContent().trim());
	}

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

	private long[] _parseCapacity(String clusterName, String regionCapacity) throws LoadConfigException {
		long[] capacity = new long[2];
		String[] strCapacity = regionCapacity.split("\\-");
		if (strCapacity.length != 2) {
			throw new LoadConfigException("解析集群容量错误");
		}

		long start = -1, end = -1;
		try {
			start = Long.parseLong(strCapacity[0]);
			end = Long.parseLong(strCapacity[1]);
		} catch (Exception e) {
			throw new LoadConfigException("解析集群容量错误, clusterName=" + clusterName, e);
		}

		if (start < 0 || end < 0 || end <= start) {
			throw new LoadConfigException("集群容量参数有误, clusterName=" + clusterName + ", start=" + start + ", end=" + end);
		}

		capacity[0] = start;
		capacity[1] = end;

		return capacity;
	}

	private DBConnectionInfo _getDBConnInfo(String clusterName, Node node, EnumDBMasterSlave masterSlave)
			throws LoadConfigException {
		DBConnectionInfo dbConnInfo = null;

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
			dbConnInfo = new EnvDBConnectionInfo();
			dbConnInfo.setClusterName(clusterName);
			dbConnInfo.setMasterSlave(masterSlave);

			String envDsName = node.getTextContent().trim();
			((EnvDBConnectionInfo) dbConnInfo).setEnvDsName(envDsName);
			break;
		case APP:
			dbConnInfo = new AppDBConnectionInfo();
			dbConnInfo.setClusterName(clusterName);
			dbConnInfo.setMasterSlave(masterSlave);

			String username = xmlUtil.getFirstChildByName(node, "db.username").getTextContent().trim();
			String password = xmlUtil.getFirstChildByName(node, "db.password").getTextContent().trim();
			String url = xmlUtil.getFirstChildByName(node, "db.url").getTextContent().trim();
			((AppDBConnectionInfo) dbConnInfo).setUsername(username);
			((AppDBConnectionInfo) dbConnInfo).setPassword(password);
			((AppDBConnectionInfo) dbConnInfo).setUrl(url);
			((AppDBConnectionInfo) dbConnInfo).setConnPoolInfo(_loadDbConnectInfo(connPoolNode));
			break;
		default:
			throw new LoadConfigException("catalog attribute of db-connection-pool config error, catalog = "
					+ cpCatalog + " you should be select in \"env\" or \"app\"");
		}

		dbConnInfo.check();

		return dbConnInfo;
	}

	private DBClusterInfo _getDBClusterInfo(String clusterName, Node clusterNode) throws LoadConfigException {
		DBClusterInfo dbClusterInfo = new DBClusterInfo();
		dbClusterInfo.setClusterName(clusterName);
		dbClusterInfo.setCatalog(EnumClusterCatalog.MYSQL);

		//
		// load global
		//
		Node global = xmlUtil.getFirstChildByName(clusterNode, "global");
		if (global != null) {
			// load master global
			Node masterGlobal = xmlUtil.getFirstChildByName(global, "master");
			DBConnectionInfo masterGlobalConnection = _getDBConnInfo(clusterName, masterGlobal,
					EnumDBMasterSlave.MASTER);
			dbClusterInfo.setMasterGlobalConnection(masterGlobalConnection);

			// load slave global
			List<Node> slaveGlobalList = xmlUtil.getChildByName(global, "slave");
			if (slaveGlobalList != null && !slaveGlobalList.isEmpty()) {
				List<DBConnectionInfo> slaveGlobalConnection = new ArrayList<DBConnectionInfo>();

				int slaveIndex = 0;
				for (Node slaveGlobal : slaveGlobalList) {
					slaveGlobalConnection.add(_getDBConnInfo(clusterName, slaveGlobal,
							EnumDBMasterSlave.getSlaveEnum(slaveIndex++)));
				}

				dbClusterInfo.setSlaveGlobalConnection(slaveGlobalConnection);
			}
		}

		//
		// load region
		//
		List<DBClusterRegionInfo> dbRegions = new ArrayList<DBClusterRegionInfo>();
		List<Node> regionNodeList = xmlUtil.getChildByName(clusterNode, "region");
		DBClusterRegionInfo regionInfo = null;
		for (Node regionNode : regionNodeList) {
			regionInfo = new DBClusterRegionInfo();

			// load cluster capacity
			String regionCapacity = xmlUtil.getAttributeValue(regionNode, "capacity");
			if (regionCapacity == null) {
				throw new LoadConfigException("<region>需要配置capacity属性");
			}
			long[] capacity = _parseCapacity(clusterName, regionCapacity);
			regionInfo.setStart(capacity[0]);
			regionInfo.setEnd(capacity[1]);

			// load region master
			List<DBConnectionInfo> regionMasterConnection = new ArrayList<DBConnectionInfo>();
			Node master = xmlUtil.getFirstChildByName(regionNode, "master");
			List<Node> shardingNodeList = xmlUtil.getChildByName(master, "sharding");
			for (Node shardingNode : shardingNodeList) {
				regionMasterConnection.add(_getDBConnInfo(clusterName, shardingNode, EnumDBMasterSlave.MASTER));
			}
			regionInfo.setMasterConnection(regionMasterConnection);

			// load region slave
			List<List<DBConnectionInfo>> regionSlaveConnection = new ArrayList<List<DBConnectionInfo>>();
			List<Node> slaveNodeList = xmlUtil.getChildByName(regionNode, "slave");
			int slaveIndex = 0;
			for (Node slaveNode : slaveNodeList) {
				shardingNodeList = xmlUtil.getChildByName(slaveNode, "sharding");

				List<DBConnectionInfo> slaveConnections = new ArrayList<DBConnectionInfo>();
				for (Node shardingNode : shardingNodeList) {
					slaveConnections.add(_getDBConnInfo(clusterName, shardingNode,
							EnumDBMasterSlave.getSlaveEnum(slaveIndex)));
				}

				regionSlaveConnection.add(slaveConnections);
			}
			regionInfo.setSlaveConnection(regionSlaveConnection);

			dbRegions.add(regionInfo);
		}
		dbClusterInfo.setDbRegions(dbRegions);

		return dbClusterInfo;
	}

	private void _loadDBClusterInfo() throws LoadConfigException {
		Node root = xmlUtil.getRoot();
		if (root == null) {
			throw new LoadConfigException("找不到root节点");
		}

		List<Node> clusterNodeList = xmlUtil.getChildByName(root, "cluster");

		for (Node clusterNode : clusterNodeList) {
			String catalog = xmlUtil.getAttributeValue(clusterNode, "catalog");
			if (!catalog.equalsIgnoreCase(EnumClusterCatalog.MYSQL.getValue())) {
				continue;
			}

			String name = xmlUtil.getAttributeValue(clusterNode, "name");
			dbClusterInfo.put(name, _getDBClusterInfo(name, clusterNode));
		}
	}

	private static IClusterConfig instance;

	public static IClusterConfig getInstance() throws LoadConfigException {
		if (instance == null) {
			synchronized (XmlDBClusterConfigImpl.class) {
				if (instance == null) {
					instance = new XmlDBClusterConfigImpl();
				}
			}
		}

		return instance;
	}

	public static IClusterConfig getInstance(String xmlFilePath) throws LoadConfigException {
		if (instance == null) {
			synchronized (XmlDBClusterConfigImpl.class) {
				if (instance == null) {
					instance = new XmlDBClusterConfigImpl(xmlFilePath);
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
	public Map<String, DBClusterInfo> getDBClusterInfo() {
		return dbClusterInfo;
	}

	@Override
	public String getZookeeperUrl() {
		return zkUrl;
	}

}
