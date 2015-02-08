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

package org.pinus4j.cluster.resources;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.datalayer.SQLBuilder;

/**
 * 全局数据资源.
 * 
 * @author duanbn
 * @since 1.1.0
 */
public class GlobalDBResource implements IDBResource {

    private IResourceId resId;

    private Connection conn;

	private String clusterName;

	private String dbName;

	private EnumDBMasterSlave masterSlave;

	//
	// database meta data.
	//
	private String databaseProductName;
	private String host;
	private String catalog;

	private GlobalDBResource() {
	}

	/**
	 * singleton
	 * 
	 * @param dbInfo
	 * @return
	 */
	public static IDBResource valueOf(DBInfo dbInfo) {
		FindKey findKey = new FindKey(dbInfo.getClusterName(), dbInfo.getDbName(), dbInfo.getMasterSlave());

		GlobalDBResource instance = DBResourceCache.getGlobalDBResource(findKey);
		if (instance == null) {
			synchronized (GlobalDBResource.class) {
				if (instance == null) {
					instance = new GlobalDBResource();

					instance.setClusterName(dbInfo.getClusterName());
					instance.setDatasource(dbInfo.getDatasource());
					instance.setDbName(dbInfo.getDbName());
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

					DBResourceCache.putGlobalDBResource(findKey, instance);
				}
			}
		}

		return instance;
	}

	@Override
	public DataSource getDatasource() {
		return this.datasource;
	}

	@Override
	public boolean isGlobal() {
		return true;
	}

	@Override
	public EnumDBMasterSlave getMasterSlave() {
		return this.masterSlave;
	}

	@Override
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
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

	public void setDatasource(DataSource datasource) {
		this.datasource = datasource;
	}

	public void setMasterSlave(EnumDBMasterSlave masterSlave) {
		this.masterSlave = masterSlave;
	}

	public static class FindKey {
		private String clusterName;

		private String dbName;

		private EnumDBMasterSlave masterSlave;

		public FindKey(String clusterName, String dbName, EnumDBMasterSlave masterSlave) {
			super();
			this.clusterName = clusterName;
			this.dbName = dbName;
			this.masterSlave = masterSlave;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
			result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
			result = prime * result + ((masterSlave == null) ? 0 : masterSlave.hashCode());
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
			if (masterSlave != other.masterSlave)
				return false;
			return true;
		}
	}

}
