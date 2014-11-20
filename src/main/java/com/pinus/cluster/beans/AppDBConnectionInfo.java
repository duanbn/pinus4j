package com.pinus.cluster.beans;

import java.util.Map;

import javax.sql.DataSource;

import com.pinus.exception.LoadConfigException;
import com.pinus.util.StringUtils;

/**
 * 表示一个数据库连接信息. 此类仅表示一个连接信息，并不是一个数据库连接对象.
 * 
 * @author duanbn
 */
public class AppDBConnectionInfo extends DBConnectionInfo {

	private String username;

	private String password;

	private String url;

	/**
	 * 校验对象的合法性
	 * 
	 * @return
	 */
	public boolean check() throws LoadConfigException {
		if (StringUtils.isBlank(this.username)) {
			throw new LoadConfigException("db username is empty");
		}
		if (StringUtils.isBlank(this.password)) {
			throw new LoadConfigException("db password is empty");
		}
		if (StringUtils.isBlank(this.url)) {
			throw new LoadConfigException("db url is empty");
		}
		return true;
	}

	/**
	 * 数据库连接池参数
	 */
	private Map<String, Object> connPoolInfo;

	@Override
	public String toString() {
		return "DBConnectionInfo [username=" + username + ", password=" + password + ", url=" + url + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		result = prime * result + ((username == null) ? 0 : username.hashCode());
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
		AppDBConnectionInfo other = (AppDBConnectionInfo) obj;
		if (clusterName == null) {
			if (other.clusterName != null)
				return false;
		} else if (!clusterName.equals(other.clusterName))
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		if (username == null) {
			if (other.username != null)
				return false;
		} else if (!username.equals(other.username))
			return false;
		return true;
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

	public DataSource getDatasource() {
		return datasource;
	}

	public void setDatasource(DataSource datasource) {
		this.datasource = datasource;
	}
}
