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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.pinus4j.cluster.beans.AppDBInfo;
import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.EnvDBInfo;
import org.pinus4j.cluster.container.ContainerType;
import org.pinus4j.cluster.container.DefaultContainerFactory;
import org.pinus4j.cluster.container.IContainer;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.utils.JdbcUtil;
import org.pinus4j.utils.StringUtil;

public abstract class AbstractConnectionPool implements IDBConnectionPool {

    private IContainer<DataSource> dsC = DefaultContainerFactory.createContainer(ContainerType.MAP);

    private IContainer<DBInfo>     dbInfos;

    private InitialContext         initCtx;

    public AbstractConnectionPool() {
        try {
            this.initCtx = new InitialContext();
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IContainer<DataSource> getAllDataSources() {
        return this.dsC;
    }

    @Override
    public void addDataSource(DBInfo dbInfo) throws LoadConfigException {
        DataSource datasource = null;

        if (dbInfo instanceof AppDBInfo) {
            AppDBInfo appDBInfo = (AppDBInfo) dbInfo;
            datasource = buildAppDataSource(appDBInfo);
        } else if (dbInfo instanceof EnvDBInfo) {
            EnvDBInfo envDbConnInfo = (EnvDBInfo) dbInfo;
            Connection conn = null;
            try {
                datasource = (DataSource) this.initCtx.lookup(envDbConnInfo.getEnvDsName());
                conn = datasource.getConnection();
                String dbCatalog = conn.getMetaData().getDatabaseProductName();
                envDbConnInfo.setDbCatalog(EnumDB.getEnum(dbCatalog));
            } catch (NamingException e) {
                throw new LoadConfigException("load jndi datasource failure, env name " + envDbConnInfo.getEnvDsName());
            } catch (SQLException e) {
                throw new LoadConfigException("get database catalog failure, env name " + envDbConnInfo.getEnvDsName());
            } finally {
                JdbcUtil.close(conn);
            }
        } else {
            throw new LoadConfigException("unknow db info type " + dbInfo.getClass());
        }

        dsC.put(dbInfo.getId(), datasource);
    }

    @Override
    public DataSource findDataSource(String dbInfoId) {
        return dsC.find(dbInfoId);
    }

    @Override
    public DBInfo findDBInfo(String dbInfoId) {
        return dbInfos.find(dbInfoId);
    }

    public IContainer<DBInfo> getDbInfos() {
        return dbInfos;
    }

    public void setDbInfos(IContainer<DBInfo> dbInfos) {
        this.dbInfos = dbInfos;
    }

    protected abstract DataSource buildAppDataSource(AppDBInfo appDBInfo) throws LoadConfigException;

    protected void setConnectionParam(DataSource obj, String propertyName, String value) {
        Method[] setMethods = obj.getClass().getMethods();
        for (Method setMethod : setMethods) {
            if (setMethod.getName().equals("set" + StringUtil.upperFirstLetter(propertyName))) {
                if (setMethod.getParameterTypes().length == 1) {
                    Class<?> paramType = setMethod.getParameterTypes()[0];
                    try {
                        if (paramType == Boolean.TYPE || paramType == Boolean.class) {
                            setMethod.invoke(obj, (Boolean.valueOf(value)).booleanValue());
                        } else if (paramType == Integer.TYPE || paramType == Integer.class) {
                            setMethod.invoke(obj, Integer.parseInt(value));
                        } else if (paramType == Byte.TYPE || paramType == Byte.class) {
                            setMethod.invoke(obj, Byte.parseByte(value));
                        } else if (paramType == Long.TYPE || paramType == Long.class) {
                            setMethod.invoke(obj, Long.parseLong(value));
                        } else if (paramType == Short.TYPE || paramType == Short.class) {
                            setMethod.invoke(obj, Short.valueOf(value));
                        } else if (paramType == Float.TYPE || paramType == Float.class) {
                            setMethod.invoke(obj, Float.valueOf(value));
                        } else if (paramType == Double.TYPE || paramType == Double.class) {
                            setMethod.invoke(obj, Double.valueOf(value));
                        } else if (paramType == Character.TYPE || paramType == Character.class) {
                            setMethod.invoke(obj, value.charAt(0));
                        } else {
                            setMethod.invoke(obj, value);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return;
                }
            }
        }
    }

}
