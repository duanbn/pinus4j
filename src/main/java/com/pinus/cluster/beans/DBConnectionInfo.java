package com.pinus.cluster.beans;

import javax.sql.DataSource;

import com.pinus.api.enums.EnumDBMasterSlave;
import com.pinus.exception.LoadConfigException;

public abstract class DBConnectionInfo {

	/**
	 * 集群名
	 */
	protected String clusterName;

	/**
	 * 数据源
	 */
	protected DataSource datasource;

	/**
	 * 主从中的角色.
	 */
	protected EnumDBMasterSlave masterSlave;

	public abstract boolean check() throws LoadConfigException;

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public DataSource getDatasource() {
		return datasource;
	}

	public void setDatasource(DataSource datasource) {
		this.datasource = datasource;
	}

	public EnumDBMasterSlave getMasterSlave() {
		return masterSlave;
	}

	public void setMasterSlave(EnumDBMasterSlave masterSlave) {
		this.masterSlave = masterSlave;
	}

}