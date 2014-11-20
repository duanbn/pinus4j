package com.pinus.cluster.beans;

import com.pinus.exception.LoadConfigException;
import com.pinus.util.StringUtils;

public class EnvDBConnectionInfo extends DBConnectionInfo {

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
		return "EnvDBConnectionInfo [envDsName=" + envDsName + "]";
	}

	public String getEnvDsName() {
		return envDsName;
	}

	public void setEnvDsName(String envDsName) {
		this.envDsName = envDsName;
	}

}
