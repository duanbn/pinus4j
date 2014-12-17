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

package com.pinus.cluster.beans;

import java.util.Map;

import com.pinus.api.enums.EnumDBMasterSlave;
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
		return "AppDBConnectionInfo [username=" + username + ", clusterName=" + clusterName + ", masterSlave="
				+ masterSlave + ", url=" + url + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((connPoolInfo == null) ? 0 : connPoolInfo.hashCode());
		result = prime * result + ((masterSlave == null) ? 0 : masterSlave.hashCode());
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
		if (connPoolInfo == null) {
			if (other.connPoolInfo != null)
				return false;
		} else if (!connPoolInfo.equals(other.connPoolInfo))
			return false;
		if (masterSlave != other.masterSlave)
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

	public EnumDBMasterSlave getMasterSlave() {
		return masterSlave;
	}

	public void setMasterSlave(EnumDBMasterSlave masterSlave) {
		this.masterSlave = masterSlave;
	}

	public void setConnPoolInfo(Map<String, Object> connPoolInfo) {
		this.connPoolInfo = connPoolInfo;
	}

	public Map<String, Object> getConnPoolInfo() {
		return this.connPoolInfo;
	}
}
