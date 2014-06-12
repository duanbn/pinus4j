package com.pinus.cluster;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * 通过路由算法获得的分库分表信息.
 * 
 * @author duanbn
 */
public class DB implements Comparable<DB> {

	/**
	 * jdbc数据库连接对象.
	 */
	private Connection dbConn;

	/**
	 * 集群名称.
	 */
	private String clusterName;

	/**
	 * 数据库下标.
	 */
	private int dbIndex;

	/**
	 * 被操作的表名.
	 */
	private String tableName;

	/**
	 * 需要被操作的表的下标.
	 */
	private int tableIndex;

	/**
	 * 主库集群信息.
	 */
	private IDBCluster dbCluster;

	private long start;

	private long end;

	private String databaseProductName;
	private String host;
	private String catalog;

	@Override
	public String toString() {
		StringBuilder info = new StringBuilder(databaseProductName);
		info.append(" host=" + host);
		info.append(" db=").append(catalog);
		info.append(" tableName=").append(this.tableName).append(this.tableIndex);
		info.append(" start=").append(this.start).append(" end=").append(this.end);
		return info.toString();
	}

	@Override
	public int compareTo(DB db) {
		return this.dbIndex - db.getDbIndex();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
		result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
		result = prime * result + ((databaseProductName == null) ? 0 : databaseProductName.hashCode());
		result = prime * result + ((dbCluster == null) ? 0 : dbCluster.hashCode());
		result = prime * result + dbIndex;
		result = prime * result + (int) (end ^ (end >>> 32));
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + (int) (start ^ (start >>> 32));
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
		if (dbCluster == null) {
			if (other.dbCluster != null)
				return false;
		} else if (!dbCluster.equals(other.dbCluster))
			return false;
		if (dbIndex != other.dbIndex)
			return false;
		if (end != other.end)
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (start != other.start)
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

	public Connection getDbConn() {
		return dbConn;
	}

	public void setDbConn(Connection dbConn) {
		DatabaseMetaData dbMeta;
		try {
			dbMeta = dbConn.getMetaData();
			this.databaseProductName = dbMeta.getDatabaseProductName();
			String url = dbMeta.getURL().substring(13);
			this.host = url.substring(0, url.indexOf("/"));
			this.catalog = dbConn.getCatalog();
		} catch (SQLException e) {
		}
		this.dbConn = dbConn;
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

	public IDBCluster getDbCluster() {
		return dbCluster;
	}

	public void setDbCluster(IDBCluster dbCluster) {
		this.dbCluster = dbCluster;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public int getDbIndex() {
		return dbIndex;
	}

	public void setDbIndex(int dbIndex) {
		this.dbIndex = dbIndex;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}
}
