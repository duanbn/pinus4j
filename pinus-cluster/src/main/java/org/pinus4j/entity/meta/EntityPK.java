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

package org.pinus4j.entity.meta;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 表示一个实体的主键
 * 
 * @author shanwei Jul 24, 2015 4:28:49 PM
 */
public class EntityPK implements Serializable {

    private static final long serialVersionUID = 1L;

    private PKName[]          pkNames;

    private PKValue[]         pkValues;

    private EntityPK() {
    }

    public static EntityPK valueOf(PKName[] pkNames, PKValue[] pkValues) {
        EntityPK pk = new EntityPK();
        pk.setPkNames(pkNames);
        pk.setPkValues(pkValues);
        return pk;
    }

    public PKName[] getPkNames() {
        return pkNames;
    }

    public void setPkNames(PKName[] pkNames) {
        this.pkNames = pkNames;
    }

    public PKValue[] getPkValues() {
        return pkValues;
    }

    public void setPkValues(PKValue[] pkValues) {
        this.pkValues = pkValues;
    }

    @Override
    public String toString() {
        StringBuilder info = new StringBuilder();
        for (int i = 0; i < pkNames.length; i++) {
            info.append(pkNames[i].getValue()).append("=").append(pkValues[i].getValue()).append(",");
        }
        info.deleteCharAt(info.length() - 1);
        return info.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(pkNames);
        result = prime * result + Arrays.hashCode(pkValues);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityPK other = (EntityPK) obj;
        if (!Arrays.equals(pkNames, other.pkNames))
            return false;
        if (!Arrays.equals(pkValues, other.pkValues))
            return false;
        return true;
    }

}
