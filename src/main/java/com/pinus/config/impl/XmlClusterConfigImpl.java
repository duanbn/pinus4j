package com.pinus.config.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.enums.HashAlgoEnum;
import com.pinus.config.IClusterConfig;
import com.pinus.constant.Const;
import com.pinus.exception.LoadConfigException;

public class XmlClusterConfigImpl implements IClusterConfig {

	public static final Logger LOG = Logger.getLogger(XmlClusterConfigImpl.class);

	/**
	 * 主键批量生成数
	 */
	private static int idGenerateBatch;

	/**
	 * zookeeper连接地址.
	 */
	private static String zkUrl;

	/**
	 * hash算法.
	 */
	private static HashAlgoEnum hashAlgo;

	/**
	 * 主全局库. {集群名, 连接信息}
	 */
	private static Map<String, DBConnectionInfo> masterGlobal = new HashMap<String, DBConnectionInfo>();

	/**
	 * 从全局库. {集群名, {从库号,连接信息}}
	 */
	private static Map<String, Map<Integer, DBConnectionInfo>> slaveGlobal = new HashMap<String, Map<Integer, DBConnectionInfo>>();

	/**
	 * 主库集群. {集群名, 集群信息}
	 */
	private static Map<String, DBClusterInfo> masterDbCluster = new HashMap<String, DBClusterInfo>();
	/**
	 * 从库集群. {集群名, {从库号, 集群信息}}
	 */
	private static Map<String, Map<Integer, DBClusterInfo>> slaveDbCluster = new HashMap<String, Map<Integer, DBClusterInfo>>();

	private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	private DocumentBuilder builder;

	private XmlClusterConfigImpl() throws LoadConfigException {
		InputStream is = null;
		Document xmlDoc = null;
		try {
			builder = factory.newDocumentBuilder();
			is = Thread.currentThread().getContextClassLoader().getResourceAsStream(Const.DEFAULT_CONFIG_FILENAME);
			xmlDoc = builder.parse(new InputSource(is));

			_loadIdGeneratorBatch(xmlDoc);

			_loadZkUrl(xmlDoc);

			_loadHashAlgo(xmlDoc);

			_loadMasterGlobal(xmlDoc);

			_loadSlaveGlobal(xmlDoc);

			_loadMasterDbCluster(xmlDoc);

			_loadSlaveDbCluster(xmlDoc);
		} catch (Exception e) {
			throw new LoadConfigException(e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				LOG.error(e);
			}
		}
	}

	private Node getRoot(Document xmlDoc) {
		return xmlDoc.getChildNodes().item(0);
	}

	private Node getChildNodeByName(Node parent, String nodeName) {
		NodeList nodeList = parent.getChildNodes();
		Node currentNode = null;
		for (int i = 0; i < nodeList.getLength(); i++) {
			currentNode = nodeList.item(i);
			if (currentNode.getNodeName().equals(nodeName)) {
				return currentNode;
			}
		}

		throw new RuntimeException("找不到node, parent=" + parent + ", nodeName=" + nodeName);
	}

	private List<Node> getNodeListByName(Node parent, String nodeName) {
		List<Node> list = new ArrayList<Node>();
		NodeList nodeList = parent.getChildNodes();
		Node currentNode = null;
		for (int i = 0; i < nodeList.getLength(); i++) {
			currentNode = nodeList.item(i);
			if (currentNode.getNodeName().equals(nodeName)) {
				list.add(currentNode);
			}
		}
		return list;
	}

	private void _loadIdGeneratorBatch(Document xmlDoc) throws LoadConfigException {
		Node idGeneratorBatchNode = getChildNodeByName(getRoot(xmlDoc), Const.PROP_IDGEN_BATCH);
		try {
			idGenerateBatch = Integer.parseInt(idGeneratorBatchNode.getTextContent().trim());
		} catch (NumberFormatException e) {
			throw new LoadConfigException(e);
		}
	}

	private void _loadZkUrl(Document xmlDoc) {
		Node zkUrlNode = getChildNodeByName(getRoot(xmlDoc), Const.PROP_ZK_URL);
		zkUrl = zkUrlNode.getTextContent().trim();
	}

	private void _loadHashAlgo(Document xmlDoc) {
		Node hashAlgoNode = getChildNodeByName(getRoot(xmlDoc), Const.PROP_HASH_ALGO);
		hashAlgo = HashAlgoEnum.getEnum(hashAlgoNode.getTextContent().trim());
	}

	private Map<String, Object> _loadDbConnectInfo(Document xmlDoc) throws LoadConfigException {
		Map<String, Object> map = new HashMap<String, Object>();
		Node connPoolNode = getChildNodeByName(getRoot(xmlDoc), "db-connection-pool");
		if (connPoolNode == null) {
			throw new LoadConfigException("配置信息错误，找不到db-connection-pool");
		}

		try {
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
					map.put(Const.PROP_TIMEBETWEENEVICTIONRUNSMILLIS,
							Integer.parseInt(currentNode.getTextContent().trim()));
				}
				if (currentNode.getNodeName().equals(Const.PROP_NUMTESTSPEREVICTIONRUN)) {
					map.put(Const.PROP_NUMTESTSPEREVICTIONRUN, Integer.parseInt(currentNode.getTextContent().trim()));
				}
				if (currentNode.getNodeName().equals(Const.PROP_MINEVICTABLEIDLETIMEMILLIS)) {
					map.put(Const.PROP_MINEVICTABLEIDLETIMEMILLIS,
							Integer.parseInt(currentNode.getTextContent().trim()));
				}
			}
			return map;
		} catch (Exception e) {
			throw new LoadConfigException(e);
		}
	}

	private void _loadMasterGlobal(Document xmlDoc) throws LoadConfigException {
		List<Node> clusterNodeList = getNodeListByName(getRoot(xmlDoc), "cluster");
		
		DBConnectionInfo dbConnInfo = null;
		for (Node clusterNode : clusterNodeList) {
			String clusterName = clusterNode.getAttributes().getNamedItem("name").getNodeValue();
			Node masterNode = getChildNodeByName(clusterNode, "master");
			Node globalNode = getChildNodeByName(masterNode, "global");

			String username = getChildNodeByName(globalNode, "db.username").getTextContent().trim();
			String password = getChildNodeByName(globalNode, "db.password").getTextContent().trim();
			String url = getChildNodeByName(globalNode, "db.url").getTextContent().trim();

			dbConnInfo = new DBConnectionInfo();
			dbConnInfo.setClusterName(clusterName);
			dbConnInfo.setUsername(username);
			dbConnInfo.setPassword(password);
			dbConnInfo.setUrl(url);
			dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));

			masterGlobal.put(clusterName, dbConnInfo);
		}
	}

	private void _loadSlaveGlobal(Document xmlDoc) throws LoadConfigException {
		List<Node> clusterNodeList = getNodeListByName(getRoot(xmlDoc), "cluster");
		DBConnectionInfo dbConnInfo = null;
		for (Node clusterNode : clusterNodeList) {
			String clusterName = clusterNode.getAttributes().getNamedItem("name").getNodeValue();
			List<Node> slaveNodeList = getNodeListByName(clusterNode, "slave");

			Map<Integer, DBConnectionInfo> slaveMap = new HashMap<Integer, DBConnectionInfo>();
			Node slaveGlobalNode = null;
			for (int i = 0; i < slaveNodeList.size(); i++) {
				slaveGlobalNode = slaveNodeList.get(i);
				Node globalNode = getChildNodeByName(slaveGlobalNode, "global");
				String username = getChildNodeByName(globalNode, "db.username").getTextContent().trim();
				String password = getChildNodeByName(globalNode, "db.password").getTextContent().trim();
				String url = getChildNodeByName(globalNode, "db.url").getTextContent().trim();

				dbConnInfo = new DBConnectionInfo();
				dbConnInfo.setClusterName(clusterName);
				dbConnInfo.setUsername(username);
				dbConnInfo.setPassword(password);
				dbConnInfo.setUrl(url);
				dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));

				slaveMap.put(i, dbConnInfo);
			}

			slaveGlobal.put(clusterName, slaveMap);
		}
	}

	private void _loadMasterDbCluster(Document xmlDoc) throws LoadConfigException {
		List<Node> clusterNodeList = getNodeListByName(getRoot(xmlDoc), "cluster");
		DBConnectionInfo dbConnInfo = null;
		for (Node clusterNode : clusterNodeList) {
			String clusterName = clusterNode.getAttributes().getNamedItem("name").getNodeValue();
			Node masterNode = getChildNodeByName(clusterNode, "master");

			List<Node> shardingNodeList = getNodeListByName(masterNode, "sharding");
			Node shardingNode = null;
			DBClusterInfo masterDbClusterInfo = new DBClusterInfo(Const.MSTYPE_MASTER);
			masterDbClusterInfo.setClusterName(clusterName);
			// 设置集群连接信息
			List<DBConnectionInfo> dbConnInfos = new ArrayList<DBConnectionInfo>();
			for (int i = 0; i < shardingNodeList.size(); i++) {
				shardingNode = shardingNodeList.get(i);
				String username = getChildNodeByName(shardingNode, "db.username").getTextContent().trim();
				String password = getChildNodeByName(shardingNode, "db.password").getTextContent().trim();
				String url = getChildNodeByName(shardingNode, "db.url").getTextContent().trim();

				dbConnInfo = new DBConnectionInfo();
				dbConnInfo.setClusterName(clusterName);
				dbConnInfo.setUsername(username);
				dbConnInfo.setPassword(password);
				dbConnInfo.setUrl(url);
				dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));

				dbConnInfos.add(dbConnInfo);
			}
			masterDbClusterInfo.setDbConnInfos(dbConnInfos);

			masterDbCluster.put(clusterName, masterDbClusterInfo);
		}
	}

	private void _loadSlaveDbCluster(Document xmlDoc) throws LoadConfigException {
		List<Node> clusterNodeList = getNodeListByName(getRoot(xmlDoc), "cluster");
		DBConnectionInfo dbConnInfo = null;
		for (Node clusterNode : clusterNodeList) {
			String clusterName = clusterNode.getAttributes().getNamedItem("name").getNodeValue();
			List<Node> slaveNodeList = getNodeListByName(clusterNode, "slave");

			Map<Integer, DBClusterInfo> oneSlaveMap = new HashMap<Integer, DBClusterInfo>();

			Node slaveNode = null;
			List<Node> shardingNodeList = null;
			for (int i = 0; i < slaveNodeList.size(); i++) {
				DBClusterInfo slaveDbClusterInfo = new DBClusterInfo(Const.MSTYPE_SLAVE);
				slaveDbClusterInfo.setClusterName(clusterName);
				slaveNode = slaveNodeList.get(i);
				shardingNodeList = getNodeListByName(slaveNode, "sharding");
				// 设置集群连接信息
				List<DBConnectionInfo> dbConnInfos = new ArrayList<DBConnectionInfo>();
				for (Node shardingNode : shardingNodeList) {
					String username = getChildNodeByName(shardingNode, "db.username").getTextContent().trim();
					String password = getChildNodeByName(shardingNode, "db.password").getTextContent().trim();
					String url = getChildNodeByName(shardingNode, "db.url").getTextContent().trim();

					dbConnInfo = new DBConnectionInfo();
					dbConnInfo.setClusterName(clusterName);
					dbConnInfo.setUsername(username);
					dbConnInfo.setPassword(password);
					dbConnInfo.setUrl(url);
					dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));

					dbConnInfos.add(dbConnInfo);
				}
				slaveDbClusterInfo.setDbConnInfos(dbConnInfos);
				oneSlaveMap.put(i, slaveDbClusterInfo);
			}

			slaveDbCluster.put(clusterName, oneSlaveMap);
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

	@Override
	public int getIdGeneratorBatch() {
		return idGenerateBatch;
	}

	@Override
	public String getZkUrl() {
		return zkUrl;
	}

	@Override
	public HashAlgoEnum getHashAlgo() {
		return hashAlgo;
	}

	@Override
	public Map<String, DBConnectionInfo> loadMasterGlobalInfo() {
		return masterGlobal;
	}

	@Override
	public Map<String, Map<Integer, DBConnectionInfo>> loadSlaveGlobalInfo() {
		return slaveGlobal;
	}

	@Override
	public Map<String, DBClusterInfo> loadMasterDbClusterInfo() {
		return masterDbCluster;
	}

	@Override
	public Map<String, Map<Integer, DBClusterInfo>> loadSlaveDbClusterInfo() {
		return slaveDbCluster;
	}

}
