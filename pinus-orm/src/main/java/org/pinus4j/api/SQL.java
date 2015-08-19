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

package org.pinus4j.api;

import java.util.Arrays;
import java.util.List;

import org.pinus4j.datalayer.SQLParser;

/**
 * SQL查询. sql语句中的变量使用"?"表示.
 * 
 * @author duanbn
 */
public class SQL {

    /**
     * sql语句
     */
    private String       sql;

    /**
     * 查询参数
     */
    private List<Object> params;

    private SQL() {
    }

    private SQL(String sql, List<Object> paramList) {
        this.sql = sql;
        this.params = paramList;
    }

    public static final SQL valueOf(String sql, Object... params) {
        return new SQL(sql, Arrays.asList(params));
    }
    
    public static final SQL valueOf(String sql, List<Object> paramList) {
        return new SQL(sql, paramList);
    }
    
    public List<String> getTableNames() {
        return SQLParser.parseTableName(sql);
    }

    @Override
    public String toString() {
        return this.sql;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getParams() {
        return params;
    }

}
