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

package org.pinus.cluster.beans;

import javax.sql.DataSource;

import org.pinus.api.enums.EnumDBMasterSlave;
import org.pinus.exception.LoadConfigException;

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