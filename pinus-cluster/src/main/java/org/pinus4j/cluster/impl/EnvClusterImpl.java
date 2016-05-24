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

import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.exceptions.LoadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class EnvClusterImpl extends AbstractDBCluster {

    public static final Logger LOG = LoggerFactory.getLogger(EnvClusterImpl.class);

    private Context            initCtx;

    /**
     * 构造方法.
     * 
     * @param enumDb 数据库类型
     */
    public EnvClusterImpl(EnumDB enumDb) {
        super(enumDb);

        try {
            this.initCtx = new InitialContext();
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void buildDataSource(DBInfo dbConnInfo) throws LoadConfigException {
        EnvDBInfo envDbConnInfo = (EnvDBInfo) dbConnInfo;

        LOG.info(envDbConnInfo.toString());

        try {
            DataSource ds = (DataSource) this.initCtx.lookup(envDbConnInfo.getEnvDsName());
            dbConnInfo.setDatasource(ds);
        } catch (NamingException e) {
            throw new LoadConfigException("load jndi datasource failure, env name " + envDbConnInfo.getEnvDsName());
        }
    }

    @Override
    public void closeDataSource(DBInfo dbConnInfo) {
        // 由容器负责关闭数据库连接
    }

}
