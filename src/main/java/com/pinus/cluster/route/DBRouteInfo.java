package com.pinus.cluster.route;

/**
 * 表示一次路由操作的结果.
 * 
 * @author duanbn
 */
public class DBRouteInfo {

	/**
	 * 集群名称. 等于不带下标的数据库名
	 */
	private String clusterName;

	/**
	 * 集群下标
	 */
	private int regionIndex;

	/**
	 * 被选中的数据库下标.
	 */
	private int dbIndex;

	/**
	 * 数据表名.
	 */
	private String tableName;

	/**
	 * 数据表下标.
	 */
	private int tableIndex;

	@Override
	public String toString() {
		return "DBRouteInfo [clusterName=" + clusterName + ", regionIndex=" + regionIndex + ", dbIndex=" + dbIndex
				+ ", tableName=" + tableName + ", tableIndex=" + tableIndex + "]";
	}

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

	public int getDbIndex() {
		return dbIndex;
	}

	public void setDbIndex(int dbIndex) {
		this.dbIndex = dbIndex;
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
}
