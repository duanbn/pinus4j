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

package org.pinus4j.cluster.impl;

import org.pinus4j.cluster.ITableCluster;
import org.pinus4j.cluster.beans.TableNumberInfo;
import org.pinus4j.cluster.container.ContainerType;
import org.pinus4j.cluster.container.DefaultContainerFactory;
import org.pinus4j.cluster.container.IContainer;

/**
 * table name is prefix by number.
 *
 * @author duanbn
 * @since 1.0.0
 */
public class NumberIndexTableCluster implements ITableCluster {

    //private Map<String, TableNumberInfo> meta;
    private IContainer<TableNumberInfo> tableNumberInfoC;

    public NumberIndexTableCluster() {
        //this.meta = new ConcurrentHashMap<String, TableNumberInfo>();
        this.tableNumberInfoC = DefaultContainerFactory.createContainer(ContainerType.MAP);
    }

    @Override
    public int getTableNumber(String clusterName, String tableName) {
        TableNumberInfo tableNumberInfo = this.tableNumberInfoC.find(clusterName);

        if (tableNumberInfo == null) {
            throw new RuntimeException("can not found table number info in " + clusterName);
        }

        return tableNumberInfo.get(tableName);
    }

    public void addTableNumberInfo(String clusterName, TableNumberInfo tableNumberInfo) {
        this.tableNumberInfoC.put(clusterName, tableNumberInfo);
    }

    public TableNumberInfo getTableNumberInfo(String clusterName) {
        return this.tableNumberInfoC.find(clusterName);
    }

}
