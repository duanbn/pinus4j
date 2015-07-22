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

package org.pinus4j.cluster.beans;

import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.StringUtils;

public class EnvDBInfo extends DBInfo {

	private String envDsName;

	@Override
	public boolean check() throws LoadConfigException {
		if (StringUtils.isBlank(envDsName)) {
			throw new LoadConfigException("env ds name is empty");
		}
		return true;
	}

	@Override
	public String toString() {
		return "EnvDBConnectionInfo [clusterName=" + clusterName + ", masterSlave=" + masterSlave + ", envDsName="
				+ envDsName + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((envDsName == null) ? 0 : envDsName.hashCode());
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
		EnvDBInfo other = (EnvDBInfo) obj;
		if (envDsName == null) {
			if (other.envDsName != null)
				return false;
		} else if (!envDsName.equals(other.envDsName))
			return false;
		return true;
	}

	public String getEnvDsName() {
		return envDsName;
	}

	public void setEnvDsName(String envDsName) {
		this.envDsName = envDsName;
	}

}
