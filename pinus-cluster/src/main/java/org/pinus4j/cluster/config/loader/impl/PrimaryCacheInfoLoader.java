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

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.beans.PrimaryCacheInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.constant.Const;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtil;
import org.w3c.dom.Node;

public class PrimaryCacheInfoLoader extends AbstractXMLConfigLoader<PrimaryCacheInfo> {

    @SuppressWarnings("unchecked")
    @Override
    public PrimaryCacheInfo load(Node xmlNode) throws LoadConfigException {
        Node dbClusterCacheNode = xmlUtil.getFirstChildByName(xmlNode, Const.PROP_DB_CLUSTER_CACHE);
        if (dbClusterCacheNode == null) {
            return null;
        }

        Node primaryNode = xmlUtil.getFirstChildByName(dbClusterCacheNode, Const.PROP_DB_CLUSTER_CACHE_PRIMARY);
        int primaryCacheExpire = Integer.parseInt(xmlUtil.getAttributeValue(primaryNode, "expire"));
        String classFullPath = xmlUtil.getAttributeValue(primaryNode, "class");
        if (StringUtil.isBlank(classFullPath)) {
            classFullPath = IClusterConfig.DEFAULT_PRIMARY_CACHE_CLASS;
        }
        Class<IPrimaryCache> primaryCacheClass;
        try {
            primaryCacheClass = (Class<IPrimaryCache>) Class.forName(classFullPath);
        } catch (ClassNotFoundException e) {
            throw new LoadConfigException("load primary cache info error", e);
        }
        Node primaryAddressNode = xmlUtil.getFirstChildByName(primaryNode, Const.PROP_DB_CLUSTER_CACHE_ADDRESS);
        String primaryCacheAddress = primaryAddressNode.getTextContent().trim();
        PrimaryCacheInfo primaryCacheInfo = new PrimaryCacheInfo();
        primaryCacheInfo.setPrimaryCacheAddress(primaryCacheAddress);
        primaryCacheInfo.setPrimaryCacheClass(primaryCacheClass);
        primaryCacheInfo.setPrimaryCacheExpire(primaryCacheExpire);
        Map<String, String> attrMap = xmlUtil.getAttributeAsMap(primaryNode, "expire", "class");
        primaryCacheInfo.setPrimaryCacheAttr(attrMap);

        return primaryCacheInfo;
    }

}
