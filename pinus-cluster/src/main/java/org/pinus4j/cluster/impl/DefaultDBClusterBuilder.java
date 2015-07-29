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

import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.IDBClusterBuilder;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.cluster.config.impl.XmlClusterConfigImpl;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.LoadConfigException;

/**
 * @author duanbn
 */
public class DefaultDBClusterBuilder implements IDBClusterBuilder {

    private EnumDB         enumDB = EnumDB.MYSQL;

    private EnumSyncAction syncAction;

    private String         scanPackage;

    @Override
    public IDBCluster build() {
        IDBCluster dbCluster = null;
        try {
            IClusterConfig config = XmlClusterConfigImpl.getInstance();

            IDBConnectionPool cp = config.getImplConnectionPool();
            dbCluster = new DBClusterImpl(enumDB, cp);

            // 设置生成数据库表动作
            dbCluster.setSyncAction(this.syncAction);
            // 设置扫描对象的包
            dbCluster.setScanPackage(this.scanPackage);
            // 启动集群
            dbCluster.startup();
        } catch (DBClusterException e) {
            throw new RuntimeException(e);
        } catch (LoadConfigException e) {
            throw new RuntimeException(e);
        }

        return dbCluster;
    }

    @Override
    public void setDbType(EnumDB enumDB) {
        this.enumDB = enumDB;
    }

    @Override
    public void setSyncAction(EnumSyncAction syncAction) {
        this.syncAction = syncAction;
    }

    @Override
    public void setScanPackage(String scanPackage) {
        this.scanPackage = scanPackage;
    }

}
