package com.pinus.cluster.beans;

import java.util.List;

/**
 * 集群区块. 一个区块中存在多个分片。
 * 
 * @author duanbn
 * 
 */
public class DBClusterRegionInfo {

	private long start;

	private long end;

	private List<DBConnectionInfo> masterConnection;

	private List<List<DBConnectionInfo>> slaveConnection;

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

	public List<DBConnectionInfo> getMasterConnection() {
		return masterConnection;
	}

	public void setMasterConnection(List<DBConnectionInfo> masterConnection) {
		this.masterConnection = masterConnection;
	}

	public List<List<DBConnectionInfo>> getSlaveConnection() {
		return slaveConnection;
	}

	public void setSlaveConnection(List<List<DBConnectionInfo>> slaveConnection) {
		this.slaveConnection = slaveConnection;
	}

}
