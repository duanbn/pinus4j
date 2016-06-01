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

package org.pinus4j.cluster.cp.impl;

import java.util.Map;

import javax.sql.DataSource;

import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.BeansUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * cluster connection pool implement by druid.
 * 
 * @author shanwei Jul 28, 2015 3:44:47 PM
 */
public class DruidConnectionPoolImpl extends AbstractConnectionPool {

    public static Logger LOG = LoggerFactory.getLogger(DruidConnectionPoolImpl.class);

    @Override
    public void releaseDataSource(DataSource datasource) {
        ((DruidDataSource) datasource).close();
    }

    @Override
    public DataSource buildAppDataSource(AppDBInfo dbInfo) throws LoadConfigException {
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName(dbInfo.getDbCatalog().getDriverClass());
        ds.setUsername(dbInfo.getUsername());
        ds.setPassword(dbInfo.getPassword());
        ds.setUrl(dbInfo.getUrl());

        // 设置连接池信息
        for (Map.Entry<String, String> entry : dbInfo.getConnPoolInfo().entrySet()) {
            if (entry.getKey().equals("passwordCallback")) {
                try {
                    BeansUtil.setProperty(ds, entry.getKey(), Class.forName(entry.getValue()).newInstance());
                } catch (Exception e) {
                    throw new LoadConfigException("create password callback instance failure");
                }
            } else {
                try {
                    setConnectionParam(ds, entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    throw new LoadConfigException(e);
                }
            }
        }
        ds.setValidationQuery("SELECT 1");

        return ds;
    }

}
