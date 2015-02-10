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

import org.pinus4j.api.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.beans.DBClusterRegionInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * 表示一个数据分片资源.
 * 
 * @author duanbn
 */
public class ShardingDBResource extends AbstractXADBResource {

	private IResourceId resId;

	/**
	 * jdbc data source.
	 */
	private Connection connection;

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
			int tableIndex) throws SQLException {
		IResourceId resId = new DBResourceId(dbInfo.getClusterName(), dbInfo.getDbName(), regionInfo.getStart(),
				regionInfo.getEnd(), tableName, tableIndex, dbInfo.getMasterSlave());

		ShardingDBResource instance = (ShardingDBResource) DBResourceCache.getShardingDBResource(resId);

		Connection conn = dbInfo.getDatasource().getConnection();
		conn.setAutoCommit(false);

		if (instance == null) {
			instance = new ShardingDBResource();

			instance.setId(resId);
			instance.setClusterName(dbInfo.getClusterName());
			instance.setDbName(dbInfo.getDbName());
			instance.setTableName(tableName);
			instance.setTableIndex(tableIndex);
			instance.setRegionStart(regionInfo.getStart());
			instance.setRegionEnd(regionInfo.getEnd());
			instance.setMasterSlave(dbInfo.getMasterSlave());

			// get database meta info.
			DatabaseMetaData dbMeta = conn.getMetaData();
			String databaseProductName = dbMeta.getDatabaseProductName();
			String url = dbMeta.getURL().substring(13);
			String host = url.substring(0, url.indexOf("/"));
			String catalog = conn.getCatalog();

			instance.setDatabaseProductName(databaseProductName);
			instance.setHost(host);
			instance.setCatalog(catalog);

			DBResourceCache.putShardingDBResource(resId, instance);
		}

		instance.setConnection(conn);

		return instance;
	}

	public void setId(IResourceId resId) {
		this.resId = resId;
	}

	@Override
	public IResourceId getId() {
		return this.resId;
	}

	@Override
	public void setTransactionIsolationLevel(EnumTransactionIsolationLevel txLevel) {
		try {
			this.connection.setTransactionIsolation(txLevel.getLevel());
		} catch (SQLException e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public Connection getConnection() {
		return this.connection;
	}

	public void setConnection(Connection conn) {
		this.connection = conn;
	}

	@Override
	public void commit() {
		try {
			this.connection.commit();
		} catch (SQLException e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public void rollback() {
		try {
			this.connection.rollback();
		} catch (SQLException e) {
			throw new DBOperationException(e);
		}
	}

	@Override
	public void close() {
		try {
			if (!this.connection.isClosed()) {
				this.connection.close();
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		}
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

}
