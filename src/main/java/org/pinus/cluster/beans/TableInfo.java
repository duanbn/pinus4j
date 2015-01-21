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

package org.pinus.cluster.beans;

/**
 * cluster table info.
 * include table name and table number.
 *
 * @author duanbn
 * @since 1.0.0
 */
public class TableInfo {

    /**
     * table name without index.
     */
    private String tableName;

    /**
     * table sharding number.
     */
    private int tableNumber;

    private TableInfo() {
    }

    public static TableInfo valueOf(String tableName) {
        return valueOf(tableName, 1);
    }

    public static TableInfo valueOf(String tableName, int tableNumber) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(tableName);
        tableInfo.setTableNumber(tableNumber);
        return tableInfo;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public int getTableNumber() {
        return tableNumber;
    }
    
    public void setTableNumber(int tableNumber) {
        this.tableNumber = tableNumber;
    }
}
