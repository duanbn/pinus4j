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

package org.pinus4j.cluster;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.pinus4j.cluster.beans.DBClusterRegionInfo;
import org.pinus4j.datalayer.SQLBuilder;

/**
 * 通过路由算法获得的分库分表信息.
 * 
 * @author duanbn
 */
public class DB {

	/**
	 * jdbc data source.
	 */
	private DataSource datasource;

	/**
	 * cluster name.
	 */
	private String clusterName;
	
	/**
	 * database name.
	 */
	private String dbName;

	/**
	 * table name.
	 */
	private String tableName;

	/**
	 * 需要被操作的表的下标.
	 */
	private int tableIndex;

	private DBClusterRegionInfo regionInfo;

	private String databaseProductName;
	private String host;
	private String catalog;

	private DB() {
	}

	public static DB valueOf(DataSource ds, String clusterName, String dbName, String tableName, int tableIndex,
			DBClusterRegionInfo regionInfo) {
		DB db = new DB();
		db.setDatasource(ds);
		db.setClusterName(clusterName);
		db.setDbName(dbName);
		db.setTableName(tableName);
		db.setTableIndex(tableIndex);
		db.setRegionInfo(regionInfo);

		return db;
	}

	@Override
	public String toString() {
		StringBuilder info = new StringBuilder();
		info.append(databaseProductName);
		info.append(" host=" + host);
		info.append(" db=").append(catalog);
		info.append(" tableName=").append(this.tableName).append(this.tableIndex);
		info.append(" start=").append(this.regionInfo.getStart()).append(" end=").append(this.regionInfo.getEnd());
		return info.toString();
	}

	public DataSource getDatasource() {
		return datasource;
	}

	public void setDatasource(DataSource datasource) {
		DatabaseMetaData dbMeta;
		Connection dbConn = null;
		try {
			dbConn = datasource.getConnection();
			dbMeta = dbConn.getMetaData();
			this.databaseProductName = dbMeta.getDatabaseProductName();
			String url = dbMeta.getURL().substring(13);
			this.host = url.substring(0, url.indexOf("/"));
			this.catalog = dbConn.getCatalog();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			SQLBuilder.close(dbConn);
		}
		this.datasource = datasource;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getTableIndex() {
		return tableIndex;
	}

	public void setTableIndex(int tableIndex) {
		this.tableIndex = tableIndex;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public DBClusterRegionInfo getRegionInfo() {
		return regionInfo;
	}

	public void setRegionInfo(DBClusterRegionInfo regionInfo) {
		this.regionInfo = regionInfo;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
		result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
		result = prime * result + ((databaseProductName == null) ? 0 : databaseProductName.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((regionInfo == null) ? 0 : regionInfo.hashCode());
		result = prime * result + tableIndex;
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DB other = (DB) obj;
		if (catalog == null) {
			if (other.catalog != null)
				return false;
		} else if (!catalog.equals(other.catalog))
			return false;
		if (clusterName == null) {
			if (other.clusterName != null)
				return false;
		} else if (!clusterName.equals(other.clusterName))
			return false;
		if (databaseProductName == null) {
			if (other.databaseProductName != null)
				return false;
		} else if (!databaseProductName.equals(other.databaseProductName))
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (regionInfo == null) {
			if (other.regionInfo != null)
				return false;
		} else if (!regionInfo.equals(other.regionInfo))
			return false;
		if (tableIndex != other.tableIndex)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

}
