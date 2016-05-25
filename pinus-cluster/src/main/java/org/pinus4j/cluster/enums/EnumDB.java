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

package org.pinus4j.cluster.enums;

import java.util.Map;

import org.pinus4j.utils.StringUtil;

import com.google.common.collect.Maps;

/**
 * Pinus存储中间件支持的数据库类型.
 *
 * @author duanbn
 */
public enum EnumDB {

    MYSQL("mysql", "com.mysql.jdbc.Driver");

    private static final Map<String, EnumDB> MAP = Maps.newHashMap();

    static {
        for (EnumDB v : EnumDB.values()) {
            MAP.put(v.getName(), v);
        }
    }

    private String                           name;

    /**
     * 数据库驱动.
     */
    private String                           driverClass;

    private EnumDB(String name, String driverClass) {
        this.name = name;
        this.driverClass = driverClass;
    }

    public String getName() {
        return name;
    }

    /**
     * 获取驱动.
     *
     * @return 驱动
     */
    public String getDriverClass() {
        return this.driverClass;
    }

    public static EnumDB getEnum(String name) {
        if (StringUtil.isNotBlank(name)) {
            return MAP.get(name.toLowerCase());
        }

        return null;
    }
}
