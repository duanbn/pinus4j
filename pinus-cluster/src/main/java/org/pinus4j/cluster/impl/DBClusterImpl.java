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

package org.pinus4j.cluster.impl;

import java.sql.Connection;

import javax.sql.DataSource;

import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于DBCP连接池的数据库集群实现.
 * 
 * @author duanbn
 */
public class DBClusterImpl extends AbstractDBCluster {

    /**
     * 日志
     */
    public static final Logger LOG = LoggerFactory.getLogger(DBClusterImpl.class);

    /**
     * 构造方法.
     * 
     * @param enumDb 数据库类型
     */
    public DBClusterImpl(IDBConnectionPool dbConnectionPool) {
        super(dbConnectionPool);
    }

    @Override
    public void buildDataSource(DBInfo dbInfo) throws LoadConfigException {

        dbInfo.setDatasource(dbConnectionPool.findDataSource(dbInfo.getId()));

        _initDatabaseName(dbInfo);

        LOG.info(dbInfo.toString());
    }

    @Override
    public void closeDataSource(DBInfo dbInfo) {
        if (dbInfo instanceof AppDBInfo) {
            dbConnectionPool.releaseDataSource(dbInfo.getDatasource());
        } else if (dbInfo instanceof EnvDBInfo) {
            // close by web container
        }
    }

    private void _initDatabaseName(DBInfo dbInfo) {
        DataSource ds = dbInfo.getDatasource();

        if (ds != null) {
            Connection conn = null;
            try {
                conn = ds.getConnection();
                String dbName = conn.getCatalog();
                dbInfo.setDbName(dbName);
            } catch (Exception e) {
                throw new RuntimeException("get database name failure ", e);
            } finally {
                JdbcUtil.close(conn);
            }
        }

    }

}
