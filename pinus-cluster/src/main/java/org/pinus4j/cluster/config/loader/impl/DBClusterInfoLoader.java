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

package org.pinus4j.cluster.config.loader.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pinus4j.cluster.beans.DBClusterInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.DBRegionInfo;
import org.pinus4j.cluster.beans.DBRegionInfo.Value;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.container.IContainer;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.router.IClusterRouter;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtil;
import org.w3c.dom.Node;

import com.google.common.collect.Lists;

public class DBClusterInfoLoader extends AbstractXMLConfigLoader<List<DBClusterInfo>> {

    private IContainer<DBInfo> dbInfos;

    public DBClusterInfoLoader(IContainer<DBInfo> dbInfos) {
        this.dbInfos = dbInfos;
    }

    @Override
    public List<DBClusterInfo> load(Node xmlNode) throws LoadConfigException {
        List<DBClusterInfo> dbClusterInfos = Lists.newArrayList();

        List<Node> clusterNodeList = xmlUtil.getChildByName(xmlNode, "cluster");
        for (Node clusterNode : clusterNodeList) {
            String name = xmlUtil.getAttributeValue(clusterNode, "name");
            dbClusterInfos.add(_getDBClusterInfo(name, clusterNode));
        }

        return dbClusterInfos;
    }

    @SuppressWarnings("unchecked")
    private DBClusterInfo _getDBClusterInfo(String clusterName, Node clusterNode) throws LoadConfigException {
        DBClusterInfo dbClusterInfo = new DBClusterInfo();
        // set cluster name
        dbClusterInfo.setClusterName(clusterName);

        // set router class
        try {
            String classFullPath = xmlUtil.getAttributeValue(clusterNode, "router");
            if (StringUtil.isBlank(classFullPath)) {
                classFullPath = IClusterConfig.DEFAULT_CLUSTER_ROUTER_CLASS;
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
            if (dbInfos.find(dsId) == null) {
                throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
            }
            DBInfo masterGlobalDBInfo = dbInfos.find(dsId).clone();
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
                    if (dbInfos.find(dsId) == null) {
                        throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
                    }
                    DBInfo slaveGlobalDBInfo = dbInfos.find(dsId).clone();
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
                if (dbInfos.find(dsId) == null) {
                    throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
                }
                DBInfo masterShardingDBInfo = dbInfos.find(dsId).clone();
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
                    if (dbInfos.find(dsId) == null) {
                        throw new LoadConfigException("配置错误，找不到datasource id=" + dsId);
                    }
                    DBInfo slaveShardingDBInfo = dbInfos.find(dsId).clone();
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

}
