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

import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtil;
import org.w3c.dom.Node;

public class ConnectionPoolClassLoader extends AbstractXMLConfigLoader<String> {

    @Override
    public String load(Node xmlNode) throws LoadConfigException {
        Node dsBucket = xmlUtil.getFirstChildByName(xmlNode, "datasource-bucket");
        if (dsBucket == null) {
            throw new LoadConfigException("can not found <datasource-bucket>");
        }

        String cpClassFullpath = xmlUtil.getAttributeValue(dsBucket, "cpclass");
        if (StringUtil.isNotBlank(cpClassFullpath)) {
            return cpClassFullpath;
        } else {
            return IClusterConfig.DEFAULT_CP_CLASS;
        }

    }

}
