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

import javax.sql.DataSource;

import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.pinus4j.utils.StringUtil;

public abstract class AbstractConnectionPool implements IDBConnectionPool {

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
