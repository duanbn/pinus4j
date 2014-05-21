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
	 * 主库集群. {集群名, 集群信息}
	 */
	private static Map<String, List<DBClusterInfo>> masterDbCluster = new HashMap<String, List<DBClusterInfo>>();
	/**
	 * 从库集群. {集群名, {从集群信息}}
	 */
	private static Map<String, List<List<DBClusterInfo>>> slaveDbCluster = new HashMap<String, List<List<DBClusterInfo>>>();

	private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	private DocumentBuilder builder;

	private XmlClusterConfigImpl() throws LoadConfigException {
		InputStream is = null;
		Document xmlDoc = null;
		try {
			builder = factory.newDocumentBuilder();
			is = Thread.currentThread().getContextClassLoader().getResourceAsStream(Const.DEFAULT_CONFIG_FILENAME);
			xmlDoc = builder.parse(new InputSource(is));

			// 加载id生成器
			_loadIdGeneratorBatch(xmlDoc);

			// 加载zookeeper
			_loadZkUrl(xmlDoc);

			// 加载hash algo
			_loadHashAlgo(xmlDoc);

			// 加载主库集群
			_loadMasterDbCluster(xmlDoc);

			// 加载从库集群
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

	private long[] parseCapacity(String clusterName, String clusterCapacity) throws LoadConfigException {
		long[] capacity = new long[2];
		String[] strCapacity = clusterCapacity.split("\\-");
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

	private DBConnectionInfo _getDBConnInfo(Node node) {
		DBConnectionInfo dbConnInfo = new DBConnectionInfo();

		String username = getChildNodeByName(node, "db.username").getTextContent().trim();
		String password = getChildNodeByName(node, "db.password").getTextContent().trim();
		String url = getChildNodeByName(node, "db.url").getTextContent().trim();

		dbConnInfo.setUsername(username);
		dbConnInfo.setPassword(password);
		dbConnInfo.setUrl(url);

		return dbConnInfo;
	}

	private void _loadMasterDbCluster(Document xmlDoc) throws LoadConfigException {
		// 获取cluster节点
		List<Node> clusterNodeList = getNodeListByName(getRoot(xmlDoc), "cluster");

		DBClusterInfo masterDbClusterInfo = null;
		for (Node clusterNode : clusterNodeList) {

			masterDbClusterInfo = new DBClusterInfo(Const.MSTYPE_MASTER);
			String clusterName = clusterNode.getAttributes().getNamedItem("name").getNodeValue();
			masterDbClusterInfo.setClusterName(clusterName);
			// 加载集群容量
			String clusterCapacity = clusterNode.getAttributes().getNamedItem("capacity").getNodeValue();
			long[] capacity = parseCapacity(clusterName, clusterCapacity);
			masterDbClusterInfo.setStart(capacity[0]);
			masterDbClusterInfo.setEnd(capacity[1]);

			Node masterNode = getChildNodeByName(clusterNode, "master");

			// 加载global db
			Node globalNode = getChildNodeByName(masterNode, "global");
			DBConnectionInfo dbConnInfo = _getDBConnInfo(globalNode);
			dbConnInfo.setClusterName(clusterName);
			dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));
			masterDbClusterInfo.setGlobalConnInfo(dbConnInfo);

			// 加载sharding db
			List<Node> shardingNodeList = getNodeListByName(masterNode, "sharding");
			Node shardingNode = null;
			List<DBConnectionInfo> dbConnInfos = new ArrayList<DBConnectionInfo>();
			for (int i = 0; i < shardingNodeList.size(); i++) {
				shardingNode = shardingNodeList.get(i);
				dbConnInfo = _getDBConnInfo(shardingNode);
				dbConnInfo.setClusterName(clusterName);
				dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));
				dbConnInfos.add(dbConnInfo);
			}
			masterDbClusterInfo.setDbConnInfos(dbConnInfos);

			if (masterDbCluster.get(clusterName) != null) {
				masterDbCluster.get(clusterName).add(masterDbClusterInfo);
			} else {
				List<DBClusterInfo> dbClusterInfos = new ArrayList<DBClusterInfo>();
				dbClusterInfos.add(masterDbClusterInfo);
				masterDbCluster.put(clusterName, dbClusterInfos);
			}
		}
	}

	private void _loadSlaveDbCluster(Document xmlDoc) throws LoadConfigException {
		List<Node> clusterNodeList = getNodeListByName(getRoot(xmlDoc), "cluster");

		DBConnectionInfo dbConnInfo = null;
		for (Node clusterNode : clusterNodeList) {
			String clusterName = clusterNode.getAttributes().getNamedItem("name").getNodeValue();

			String clusterCapacity = clusterNode.getAttributes().getNamedItem("capacity").getNodeValue();
			long[] capacity = parseCapacity(clusterName, clusterCapacity);

			List<DBClusterInfo> a = new ArrayList<DBClusterInfo>();

			List<Node> slaveNodeList = getNodeListByName(clusterNode, "slave");
			List<Node> shardingNodeList = null;
			for (Node slaveNode : slaveNodeList) {
				DBClusterInfo slaveDbClusterInfo = new DBClusterInfo(Const.MSTYPE_SLAVE);
				slaveDbClusterInfo.setClusterName(clusterName);
				slaveDbClusterInfo.setStart(capacity[0]);
				slaveDbClusterInfo.setEnd(capacity[1]);

				// 加载global db
				Node globalNode = getChildNodeByName(slaveNode, "global");
				dbConnInfo = _getDBConnInfo(globalNode);
				dbConnInfo.setClusterName(clusterName);
				dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));
				slaveDbClusterInfo.setGlobalConnInfo(dbConnInfo);

				// 加载sharding db
				shardingNodeList = getNodeListByName(slaveNode, "sharding");
				List<DBConnectionInfo> dbConnInfos = new ArrayList<DBConnectionInfo>();
				for (Node shardingNode : shardingNodeList) {
					dbConnInfo = _getDBConnInfo(shardingNode);
					dbConnInfo.setClusterName(clusterName);
					dbConnInfo.setConnPoolInfo(_loadDbConnectInfo(xmlDoc));

					dbConnInfos.add(dbConnInfo);
				}
				slaveDbClusterInfo.setDbConnInfos(dbConnInfos);

				a.add(slaveDbClusterInfo);
			}

			if (slaveDbCluster.get(clusterName) != null) {
				slaveDbCluster.get(clusterName).add(a);
			} else {
				List<List<DBClusterInfo>> c = new ArrayList<List<DBClusterInfo>>();
				c.add(a);
				slaveDbCluster.put(clusterName, c);
			}
		}

		for (Map.Entry<String, List<List<DBClusterInfo>>> entry : slaveDbCluster.entrySet()) {
			int slaveNum = entry.getValue().get(0).size();
			List<List<DBClusterInfo>> f = new ArrayList<List<DBClusterInfo>>();
			for (int i = 0; i < slaveNum; i++) {
				List<DBClusterInfo> d = new ArrayList<DBClusterInfo>();
				for (List<DBClusterInfo> e : entry.getValue()) {
					d.add(e.get(i));
				}
				f.add(d);
			}
			slaveDbCluster.put(entry.getKey(), f);
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
	public Map<String, List<DBClusterInfo>> loadMasterDbClusterInfo() {
		return masterDbCluster;
	}

	@Override
	public Map<String, List<List<DBClusterInfo>>> loadSlaveDbClusterInfo() {
		return slaveDbCluster;
	}

}
