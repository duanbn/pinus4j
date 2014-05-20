package com.pinus.cluster.beans;

import java.util.List;

/**
 * 表示一个数据库集群信息. 包含此集群是主库集群还是从库集群，集群的名称(不带下标的数据库名)，集群连接信息.
 * 
 * @author duanbn
 */
public class DBClusterInfo {

	/**
	 * 此集群是主还是从. 可选值, Const.MSTYPE_MASTER, Const.MSTYPE_SLAVE.
	 */
	private byte masterSlaveType;

	/**
	 * 数据库集群名称.
	 */
	private String clusterName;

	/**
	 * 数据库集群连接.
	 */
	private List<DBConnectionInfo> dbConnInfos;

	public DBClusterInfo(byte masterSlaveType) {
		this.masterSlaveType = masterSlaveType;
	}

	@Override
	public String toString() {
		return "DBClusterInfo [masterSlaveType=" + masterSlaveType + ", clusterName=" + clusterName + ", dbConnInfos="
				+ dbConnInfos + "]";
	}

	public byte getMasterSlaveType() {
		return masterSlaveType;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public List<DBConnectionInfo> getDbConnInfos() {
		return dbConnInfos;
	}

	public void setDbConnInfos(List<DBConnectionInfo> dbConnInfos) {
		this.dbConnInfos = dbConnInfos;
	}

}
