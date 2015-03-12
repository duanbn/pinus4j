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

import java.util.List;

import org.pinus4j.cluster.enums.EnumClusterCatalog;
import org.pinus4j.cluster.router.IClusterRouter;

/**
 * 表示一个数据库集群信息.
 * 
 * @author duanbn
 * @since 0.6.0
 */
public class DBClusterInfo {

	/**
	 * cluster name.
	 */
	private String clusterName;

    /**
     * catalog for database.
     */
	private EnumClusterCatalog catalog;

    /**
     * class of cluster router.
     * this class for create router instance.
     */
    private Class<IClusterRouter> routerClass;

	/**
	 *  master global database info.
	 */
	private DBInfo masterGlobalDBInfo;

    /**
     * slave global database info.
     */
	private List<DBInfo> slaveGlobalDBInfo;

    /**
     * sharding database info.
     */
	private List<DBRegionInfo> dbRegions;

	@Override
	public String toString() {
		return "DBClusterInfo [clusterName=" + clusterName + ", catalog=" + catalog + ", masterGlobalDBInfo="
				+ masterGlobalDBInfo + ", slaveGlobalDBInfo=" + slaveGlobalDBInfo + ", dbRegions="
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

    public void setRouterClass(Class<IClusterRouter> clazz) {
        this.routerClass = clazz;
    }

    public Class<IClusterRouter> getRouterClass() {
        return this.routerClass;
    }

	public DBInfo getMasterGlobalDBInfo() {
		return masterGlobalDBInfo;
	}

	public void setMasterGlobalDBInfo(DBInfo masterGlobalDBInfo) {
		this.masterGlobalDBInfo = masterGlobalDBInfo;
	}

	public List<DBInfo> getSlaveGlobalDBInfo() {
		return slaveGlobalDBInfo;
	}

	public void setSlaveGlobalDBInfo(List<DBInfo> slaveGlobalDBInfo) {
		this.slaveGlobalDBInfo = slaveGlobalDBInfo;
	}

	public List<DBRegionInfo> getDbRegions() {
		return dbRegions;
	}

	public void setDbRegions(List<DBRegionInfo> dbRegions) {
		this.dbRegions = dbRegions;
	}

}
