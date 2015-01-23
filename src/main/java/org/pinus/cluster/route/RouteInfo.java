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

package org.pinus.cluster.route;

import org.pinus.cluster.beans.DBInfo;

/**
 * 表示一次路由操作的结果.
 * 
 * @author duanbn
 */
public class RouteInfo {

	/**
	 * cluster name.
	 */
	private String clusterName;

	/**
	 * region index.
	 */
	private int regionIndex;

    /**
     * db info is selected.
     */
    private DBInfo dbInfo;

	/**
	 *  table name.
	 */
	private String tableName;

	/**
	 *  index of table.
	 */
	private int tableIndex;
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public int getRegionIndex() {
		return regionIndex;
	}

	public void setRegionIndex(int regionIndex) {
		this.regionIndex = regionIndex;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public DBInfo getDbInfo() {
		return dbInfo;
	}

	public void setDbInfo(DBInfo dbInfo) {
		this.dbInfo = dbInfo;
	}

	public int getTableIndex() {
		return tableIndex;
	}

	public void setTableIndex(int tableIndex) {
		this.tableIndex = tableIndex;
	}
}
