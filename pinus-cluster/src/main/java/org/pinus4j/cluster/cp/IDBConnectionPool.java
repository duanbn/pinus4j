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

package org.pinus4j.cluster.cp;

import java.util.Map;

import javax.sql.DataSource;

import org.pinus4j.exceptions.LoadConfigException;

/**
 * 数据库连接池接口. 不同的数据库连接池实现此接口
 * 
 * @author shanwei Jul 28, 2015 11:38:58 AM
 */
public interface IDBConnectionPool {

    /**
     * 释放连接
     * 
     * @param datasource
     */
    void releaseDataSource(DataSource datasource);

    /**
     * 创建数据源
     * 
     * @return
     */
    DataSource buildDataSource(String driverClass, String userName, String password, String url,
                               Map<String, String> connectParam) throws LoadConfigException;

}
