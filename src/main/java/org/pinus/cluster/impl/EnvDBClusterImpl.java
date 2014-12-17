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

package org.pinus.cluster.impl;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.pinus.api.enums.EnumDB;
import org.pinus.cluster.AbstractDBCluster;
import org.pinus.cluster.beans.DBConnectionInfo;
import org.pinus.cluster.beans.EnvDBConnectionInfo;
import org.pinus.exception.LoadConfigException;

public class EnvDBClusterImpl extends AbstractDBCluster {

	public static final Logger LOG = Logger.getLogger(EnvDBClusterImpl.class);

	private Context initCtx;

	/**
	 * 构造方法.
	 * 
	 * @param enumDb
	 *            数据库类型
	 */
	public EnvDBClusterImpl(EnumDB enumDb) {
		super(enumDb);

		try {
			this.initCtx = new InitialContext();
		} catch (NamingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void buildDataSource(DBConnectionInfo dbConnInfo) throws LoadConfigException {
		EnvDBConnectionInfo envDbConnInfo = (EnvDBConnectionInfo) dbConnInfo;

		LOG.info(envDbConnInfo);

		try {
			DataSource ds = (DataSource) this.initCtx.lookup(envDbConnInfo.getEnvDsName());
			dbConnInfo.setDatasource(ds);
		} catch (NamingException e) {
			throw new LoadConfigException("load jndi datasource failure, env name " + envDbConnInfo.getEnvDsName());
		}
	}

	@Override
	public void closeDataSource(DBConnectionInfo dbConnInfo) {
		// 由容器负责关闭数据库连接
	}

}
