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

import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cache.beans.SecondCacheInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.constant.Const;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtil;
import org.w3c.dom.Node;

public class SecondCacheInfoLoader extends AbstractXMLConfigLoader<SecondCacheInfo> {

    @SuppressWarnings("unchecked")
    @Override
    public SecondCacheInfo load(Node xmlNode) throws LoadConfigException {
        Node dbClusterCacheNode = xmlUtil.getFirstChildByName(xmlNode, Const.PROP_DB_CLUSTER_CACHE);
        if (dbClusterCacheNode == null) {
            return null;
        }

        Node secondNode = xmlUtil.getFirstChildByName(dbClusterCacheNode, Const.PROP_DB_CLUSTER_CACHE_SECOND);
        int secondCacheExpire = Integer.parseInt(xmlUtil.getAttributeValue(secondNode, "expire"));
        String classFullPath = xmlUtil.getAttributeValue(secondNode, "class");
        if (StringUtil.isBlank(classFullPath)) {
            classFullPath = IClusterConfig.DEFAULT_SECOND_CACHE_CLASS;
        }
        Class<ISecondCache> secondCacheClass;
        try {
            secondCacheClass = (Class<ISecondCache>) Class.forName(classFullPath);
        } catch (ClassNotFoundException e) {
            throw new LoadConfigException("load second cache info error", e);
        }
        Node secondAddressNode = xmlUtil.getFirstChildByName(secondNode, Const.PROP_DB_CLUSTER_CACHE_ADDRESS);
        String secondCacheAddress = secondAddressNode.getTextContent().trim();
        SecondCacheInfo secondCacheInfo = new SecondCacheInfo();
        secondCacheInfo.setSecondCacheAddress(secondCacheAddress);
        secondCacheInfo.setSecondCacheClass(secondCacheClass);
        secondCacheInfo.setSecondCacheExpire(secondCacheExpire);
        Map<String, String> attrMap = xmlUtil.getAttributeAsMap(secondNode, "expire", "class");
        secondCacheInfo.setSecondCacheAttr(attrMap);

        return secondCacheInfo;
    }

}
