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

import java.util.Map;

import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.container.ContainerType;
import org.pinus4j.cluster.container.DefaultContainerFactory;
import org.pinus4j.cluster.container.IContainer;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtil;
import org.w3c.dom.Node;

public class DataSourceBucketLoader extends AbstractXMLConfigLoader<IContainer<DBInfo>> {

    @Override
    public IContainer<DBInfo> load(Node xmlNode) throws LoadConfigException {
        IContainer<DBInfo> dbInfos = DefaultContainerFactory.createContainer(ContainerType.MAP);

        Node dsBucket = xmlUtil.getFirstChildByName(xmlNode, "datasource-bucket");
        if (dsBucket == null) {
            throw new LoadConfigException("can not found <datasource-bucket>");
        }

        String dbInfoId = null;
        DBInfo dbInfo = null;

        for (Node appDBInfoNode : xmlUtil.getChildByName(dsBucket, "appds")) {
            dbInfoId = xmlUtil.getAttributeValue(appDBInfoNode, "id");
            if (StringUtil.isBlank(dbInfoId))
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

            if (dbInfos.find(dbInfoId) != null) {
                throw new LoadConfigException("配置错误，数据源id配置重复，id=" + dbInfoId);
            }
            dbInfos.put(dbInfoId, dbInfo);
        }

        for (Node envDBInfoNode : xmlUtil.getChildByName(dsBucket, "envds")) {
            dbInfo = new EnvDBInfo();
            String envDsName = xmlUtil.getFirstChildByName(envDBInfoNode, "jndi").getTextContent().trim();
            ((EnvDBInfo) dbInfo).setEnvDsName(envDsName);

            if (dbInfos.find(dbInfoId) != null) {
                throw new LoadConfigException("配置错误，数据源id配置重复，id=" + dbInfoId);
            }
            dbInfos.put(dbInfoId, dbInfo);
        }

        return dbInfos;
    }

}
