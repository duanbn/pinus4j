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

import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.beans.DBClusterRegionInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.datalayer.SQLBuilder;

/**
 * 表示一个数据分片资源.
 * 
 * @author duanbn
 */
public class ShardingDBResource implements IDBResource {

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
	 * table name without index.
	 */
	private String tableName;

	/**
	 * index of table
	 */
	private int tableIndex;

	/**
	 * region start
	 */
	private long regionStart;

	/**
	 * region end.
	 */
	private long regionEnd;

	private EnumDBMasterSlave masterSlave;

	//
	// database meta data.
	//
	private String databaseProductName;
	private String host;
	private String catalog;

	private ShardingDBResource() {
	}

	public static ShardingDBResource valueOf(DBInfo dbInfo, DBClusterRegionInfo regionInfo, String tableName,
			int tableIndex) {
		FindKey findKey = new FindKey(dbInfo.getClusterName(), dbInfo.getDbName(), tableName, tableIndex,
				regionInfo.getStart(), regionInfo.getEnd());

		ShardingDBResource instance = DBResourceCache.getShardingDBResource(findKey);

		if (instance == null) {
			synchronized (ShardingDBResource.class) {
				if (instance == null) {
					instance = new ShardingDBResource();

					instance.setClusterName(dbInfo.getClusterName());
					instance.setDbName(dbInfo.getDbName());
					instance.setTableName(tableName);
					instance.setTableIndex(tableIndex);
					instance.setRegionStart(regionInfo.getStart());
					instance.setRegionEnd(regionInfo.getEnd());
					instance.setMasterSlave(dbInfo.getMasterSlave());

					// get database meta info.
					DatabaseMetaData dbMeta;
					Connection dbConn = null;
					try {
						dbConn = dbInfo.getDatasource().getConnection();
						dbMeta = dbConn.getMetaData();
						String databaseProductName = dbMeta.getDatabaseProductName();
						String url = dbMeta.getURL().substring(13);
						String host = url.substring(0, url.indexOf("/"));
						String catalog = dbConn.getCatalog();

						instance.setDatabaseProductName(databaseProductName);
						instance.setHost(host);
						instance.setCatalog(catalog);
						instance.setDatasource(dbInfo.getDatasource());
					} catch (SQLException e) {
						throw new RuntimeException(e);
					} finally {
						SQLBuilder.close(dbConn);
					}

					DBResourceCache.putShardingDBResource(findKey, instance);
				}
			}

		}

		return instance;
	}

	@Override
	public String getClusterName() {
		return clusterName;
	}

	@Override
	public boolean isGlobal() {
		return false;
	}

	@Override
	public EnumDBMasterSlave getMasterSlave() {
		return this.masterSlave;
	}

	@Override
	public String toString() {
		StringBuilder info = new StringBuilder();
		info.append(databaseProductName);
		info.append(" host=" + host);
		info.append(" db=").append(catalog);
		info.append(" tableName=").append(this.tableName).append(this.tableIndex);
		info.append(" start=").append(this.regionStart).append(" end=").append(this.regionEnd);
		return info.toString();
	}

	public DataSource getDatasource() {
		return datasource;
	}

	public void setDatasource(DataSource datasource) {
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

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public long getRegionStart() {
		return regionStart;
	}

	public void setRegionStart(long regionStart) {
		this.regionStart = regionStart;
	}

	public long getRegionEnd() {
		return regionEnd;
	}

	public void setRegionEnd(long regionEnd) {
		this.regionEnd = regionEnd;
	}

	public String getDatabaseProductName() {
		return databaseProductName;
	}

	public void setDatabaseProductName(String databaseProductName) {
		this.databaseProductName = databaseProductName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public void setMasterSlave(EnumDBMasterSlave masterSlave) {
		this.masterSlave = masterSlave;
	}

	public static class FindKey {
		private String clusterName;

		private String dbName;

		private String tableName;

		private int tableIndex;

		private long regionStart;

		private long regionEnd;

		public FindKey(String clusterName, String dbName, String tableName, int tableIndex, long regionStart,
				long regionEnd) {
			super();
			this.clusterName = clusterName;
			this.dbName = dbName;
			this.tableName = tableName;
			this.tableIndex = tableIndex;
			this.regionStart = regionStart;
			this.regionEnd = regionEnd;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
			result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
			result = prime * result + (int) (regionEnd ^ (regionEnd >>> 32));
			result = prime * result + (int) (regionStart ^ (regionStart >>> 32));
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
			FindKey other = (FindKey) obj;
			if (clusterName == null) {
				if (other.clusterName != null)
					return false;
			} else if (!clusterName.equals(other.clusterName))
				return false;
			if (dbName == null) {
				if (other.dbName != null)
					return false;
			} else if (!dbName.equals(other.dbName))
				return false;
			if (regionEnd != other.regionEnd)
				return false;
			if (regionStart != other.regionStart)
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

}
