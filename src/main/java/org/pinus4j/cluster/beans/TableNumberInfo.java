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

package org.pinus4j.cluster.beans;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * include all cluster's table number info.
 *
 * @author duanbn
 * @since 1.0.0
 */
public class TableNumberInfo {

    private String clusterName;

    private Map<String, Integer> meta;

    private TableNumberInfo() {
        this.meta = new ConcurrentHashMap<String, Integer>();
    }

    public static TableNumberInfo valueOf(String clusterName) {
        TableNumberInfo tableNumberInfo = new TableNumberInfo();
        tableNumberInfo.setClusterName(clusterName);
        return tableNumberInfo;
    }

    public void add(String tableName, int tableNumber) {
        this.meta.put(tableName, tableNumber);
    }

    public int get(String tableName) {
        Integer number = this.meta.get(tableName);
        if (number == null) {
            throw new RuntimeException("not exists table " + tableName);
        }

        return number.intValue();
    }
    
    public String getClusterName() {
        return clusterName;
    }
    
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
