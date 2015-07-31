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

package org.pinus4j.cluster.resources;

import java.sql.Connection;

import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * database resource interface.
 * 
 * @author duanbn
 * @since 1.1.0
 */
public interface IDBResource {

    /**
     * get resource id.
     */
    IResourceId getId();

    /**
     * set transaction isolation level.
     * 
     * @param txLevel isolation level
     */
    void setTransactionIsolationLevel(EnumTransactionIsolationLevel txLevel);

    /**
     * get database connection.
     * 
     * @return
     */
    Connection getConnection();

    /**
     * commit.
     */
    void commit();

    /**
     * rollback.
     */
    void rollback();

    /**
     * close.
     */
    void close();

    /**
     * is closed.
     * 
     * @return
     */
    boolean isClosed();

    /**
     * cluster name of database resource.
     * 
     * @return
     */
    String getClusterName();

    /**
     * global is ture, sharding is false.
     * 
     * @return
     */
    boolean isGlobal();

    /**
     * get master slave mode.
     * 
     * @return
     */
    EnumDBMasterSlave getMasterSlave();

}
