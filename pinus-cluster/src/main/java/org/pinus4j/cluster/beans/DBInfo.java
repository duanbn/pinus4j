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

import java.util.Map;

import javax.sql.DataSource;

import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.exceptions.LoadConfigException;

/**
 * database instance info.
 *
 * @author duanbn
 * @since 1.0.0
 */
public abstract class DBInfo {

    protected String              id;

    protected EnumDB              dbCatalog;

    /**
     * 数据源
     */
    protected DataSource          datasource;

    /**
     * 集群名
     */
    protected String              clusterName;

    /**
     * database name.
     */
    protected String              dbName;

    /**
     * 主从中的角色.
     */
    protected EnumDBMasterSlave   masterSlave;

    /**
     * custom properties.
     */
    protected Map<String, String> customProperties;

    public abstract boolean check() throws LoadConfigException;

    public abstract DBInfo clone();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public EnumDB getDbCatalog() {
        return dbCatalog;
    }

    public void setDbCatalog(EnumDB dbCatalog) {
        this.dbCatalog = dbCatalog;
    }

    public DataSource getDatasource() {
        return datasource;
    }

    public void setDatasource(DataSource datasource) {
        this.datasource = datasource;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public EnumDBMasterSlave getMasterSlave() {
        return masterSlave;
    }

    public void setMasterSlave(EnumDBMasterSlave masterSlave) {
        this.masterSlave = masterSlave;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(Map<String, String> customProperties) {
        this.customProperties = customProperties;
    }
}
