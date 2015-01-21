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

package org.pinus.cluster.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pinus.cluster.ITableCluster;
import org.pinus.cluster.beans.TableNumberInfo;

/**
 * table name is prefix by number.
 *
 * @author duanbn
 * @since 1.0.0
 */
public class NumberIndexTableCluster implements ITableCluster {

    private Map<String, TableNumberInfo> meta;

	public NumberIndexTableCluster() {
        this.meta = new ConcurrentHashMap<String, TableNumberInfo>();
    }

    @Override
    public int getTableNumber(String clusterName, String tableName) {
        TableNumberInfo tableNumberInfo = this.meta.get(clusterName);

        if (tableNumberInfo == null) {
            throw new RuntimeException("can not found table number info in " + clusterName);
        }

        return tableNumberInfo.get(tableName);
    }

    public void addTableNumberInfo(String clusterName, TableNumberInfo tableNumberInfo) {
        this.meta.put(clusterName, tableNumberInfo);
    }

    public TableNumberInfo getTableNumberInfo(String clusterName) {
        return this.meta.get(clusterName);
    }

}
