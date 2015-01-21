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

import java.util.List;

import org.pinus.cluster.enums.EnumClusterCatalog;

/**
 * 表示一个数据库集群信息.
 * 
 * @author duanbn
 */
public class DBClusterInfo {

	/**
	 * 数据库集群名称.
	 */
	private String clusterName;

	private EnumClusterCatalog catalog;

	/**
	 * 集群中的全局库
	 */
	private DBInfo masterGlobalConnection;

	private List<DBInfo> slaveGlobalConnection;

	private List<DBClusterRegionInfo> dbRegions;

	@Override
	public String toString() {
		return "DBClusterInfo [clusterName=" + clusterName + ", catalog=" + catalog + ", masterGlobalConnection="
				+ masterGlobalConnection + ", slaveGlobalConnection=" + slaveGlobalConnection + ", dbRegions="
				+ dbRegions + "]";
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public EnumClusterCatalog getCatalog() {
		return catalog;
	}

	public void setCatalog(EnumClusterCatalog catalog) {
		this.catalog = catalog;
	}

	public DBInfo getMasterGlobalConnection() {
		return masterGlobalConnection;
	}

	public void setMasterGlobalConnection(DBInfo masterGlobalConnection) {
		this.masterGlobalConnection = masterGlobalConnection;
	}

	public List<DBInfo> getSlaveGlobalConnection() {
		return slaveGlobalConnection;
	}

	public void setSlaveGlobalConnection(List<DBInfo> slaveGlobalConnection) {
		this.slaveGlobalConnection = slaveGlobalConnection;
	}

	public List<DBClusterRegionInfo> getDbRegions() {
		return dbRegions;
	}

	public void setDbRegions(List<DBClusterRegionInfo> dbRegions) {
		this.dbRegions = dbRegions;
	}

}
