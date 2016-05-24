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

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.exceptions.LoadConfigException;
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

    private IDBConnectionPool  dbConnectionPool;

    private Context            initCtx;

    /**
     * 构造方法.
     * 
     * @param enumDb 数据库类型
     */
    public DBClusterImpl(EnumDB enumDb, IDBConnectionPool dbConnectionPool) {
        super(enumDb);

        this.dbConnectionPool = dbConnectionPool;

        try {
            this.initCtx = new InitialContext();
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void buildDataSource(DBInfo dbConnInfo) throws LoadConfigException {

        LOG.info(dbConnInfo.toString());

        DataSource ds = null;
        if (dbConnInfo instanceof AppDBInfo) {
            AppDBInfo appDBInfo = (AppDBInfo) dbConnInfo;
            ds = this.dbConnectionPool.buildDataSource(enumDb.getDriverClass(), appDBInfo.getUsername(),
                    appDBInfo.getPassword(), appDBInfo.getUrl(), appDBInfo.getConnPoolInfo());
        } else if (dbConnInfo instanceof EnvDBInfo) {
            EnvDBInfo envDbConnInfo = (EnvDBInfo) dbConnInfo;
            try {
                ds = (DataSource) this.initCtx.lookup(envDbConnInfo.getEnvDsName());
            } catch (NamingException e) {
                throw new LoadConfigException("load jndi datasource failure, env name " + envDbConnInfo.getEnvDsName());
            }
        } else {
            throw new LoadConfigException("unknow db info type " + dbConnInfo.getClass());
        }

        dbConnInfo.setDatasource(ds);
    }

    @Override
    public void closeDataSource(DBInfo dbInfo) {
        if (dbInfo instanceof AppDBInfo) {
            dbConnectionPool.releaseDataSource(dbInfo.getDatasource());
        } else if (dbInfo instanceof EnvDBInfo) {
            // close by web container
        }
    }

}
