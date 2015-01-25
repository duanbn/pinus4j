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

import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.pinus.api.enums.EnumDB;
import org.pinus.cluster.beans.AppDBInfo;
import org.pinus.cluster.beans.DBInfo;
import org.pinus.constant.Const;
import org.pinus.exception.LoadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于DBCP连接池的数据库集群实现.
 * 
 * @author duanbn
 */
public class AppDBClusterImpl extends AbstractDBCluster {

	public static final Logger LOG = LoggerFactory.getLogger(AppDBClusterImpl.class);

	/**
	 * 构造方法.
	 * 
	 * @param enumDb
	 *            数据库类型
	 */
	public AppDBClusterImpl(EnumDB enumDb) {
		super(enumDb);
	}

	@Override
	public void buildDataSource(DBInfo dbConnInfo) throws LoadConfigException {
		AppDBInfo appDbConnInfo = (AppDBInfo) dbConnInfo;

		LOG.info(dbConnInfo.toString());

		try {
			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(enumDb.getDriverClass());
			ds.setUsername(appDbConnInfo.getUsername());
			ds.setPassword(appDbConnInfo.getPassword());
			ds.setUrl(appDbConnInfo.getUrl());

			// 设置连接池信息
			Map<String, Object> dbConnPoolInfo = appDbConnInfo.getConnPoolInfo();
			ds.setValidationQuery("SELECT 1");
			ds.setMaxActive((Integer) dbConnPoolInfo.get(Const.PROP_MAXACTIVE));
			ds.setMinIdle((Integer) dbConnPoolInfo.get(Const.PROP_MINIDLE));
			ds.setMaxIdle((Integer) dbConnPoolInfo.get(Const.PROP_MAXIDLE));
			ds.setInitialSize((Integer) dbConnPoolInfo.get(Const.PROP_INITIALSIZE));
			ds.setRemoveAbandoned((Boolean) dbConnPoolInfo.get(Const.PROP_REMOVEABANDONED));
			ds.setRemoveAbandonedTimeout((Integer) dbConnPoolInfo.get(Const.PROP_REMOVEABANDONEDTIMEOUT));
			ds.setMaxWait((Integer) dbConnPoolInfo.get(Const.PROP_MAXWAIT));
			ds.setTimeBetweenEvictionRunsMillis((Integer) dbConnPoolInfo.get(Const.PROP_TIMEBETWEENEVICTIONRUNSMILLIS));
			ds.setNumTestsPerEvictionRun((Integer) dbConnPoolInfo.get(Const.PROP_NUMTESTSPEREVICTIONRUN));
			ds.setMinEvictableIdleTimeMillis((Integer) dbConnPoolInfo.get(Const.PROP_MINEVICTABLEIDLETIMEMILLIS));

			dbConnInfo.setDatasource(ds);
		} catch (Exception e) {
			throw new LoadConfigException(e);
		}
	}

	@Override
	public void closeDataSource(DBInfo dbConnInfo) {
		try {
			((BasicDataSource) dbConnInfo.getDatasource()).close();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
	}

}
