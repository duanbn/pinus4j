package com.pinus.cluster.beans;

import java.util.Map;

import com.pinus.util.StringUtils;

/**
 * 表示一个数据库连接信息. 此类仅表示一个连接信息，并不是一个数据库连接对象.
 * 
 * @author duanbn
 */
public class DBConnectionInfo {

	private String clusterName;

	private String username;

	private String password;

	private String url;

	/**
	 * 数据库连接池参数
	 */
	private Map<String, Object> connPoolInfo;

	public boolean check() {
		if (StringUtils.isBlank(this.username)) {
			return false;
		}
		if (StringUtils.isBlank(this.password)) {
			return false;
		}
		if (StringUtils.isBlank(this.url)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "DBConnectionInfo [clusterName=" + clusterName + ", username=" + username + ", password=" + password
				+ ", url=" + url + ", connPoolInfo=" + connPoolInfo + "]";
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public void setConnPoolInfo(Map<String, Object> connPoolInfo) {
		this.connPoolInfo = connPoolInfo;
	}

	public Map<String, Object> getConnPoolInfo() {
		return this.connPoolInfo;
	}
}
