package com.pinus.datalayer.beans;

/**
 * 集群遍历器初始化信息.
 * 
 * @author duanbn
 * @since 0.6.0
 */
public class DBClusterIteratorInfo {

	private int latestDbIndex;

	private int latestTableIndex;

	private int latestId;

	public int getLatestDbIndex() {
		return latestDbIndex;
	}

	public void setLatestDbIndex(int latestDbIndex) {
		this.latestDbIndex = latestDbIndex;
	}

	public int getLatestTableIndex() {
		return latestTableIndex;
	}

	public void setLatestTableIndex(int latestTableIndex) {
		this.latestTableIndex = latestTableIndex;
	}

	public int getLatestId() {
		return latestId;
	}

	public void setLatestId(int latestId) {
		this.latestId = latestId;
	}

}
