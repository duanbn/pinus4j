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

package org.pinus4j.utils;

import java.util.List;

import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKValue;

import com.google.common.collect.Lists;

/**
 * @author shanwei Jul 25, 2015 9:42:36 AM
 */
public class PKUtil {

    public static List<EntityPK> parseEntityPKList(List<PKValue> pkValueList) {
        List<EntityPK> entityPkList = Lists.newArrayList();

        PKValue[] pkValues = null;
        for (PKValue pkValue : pkValueList) {
            pkValues = new PKValue[] { pkValue };
            entityPkList.add(EntityPK.valueOf(null, pkValues));
        }

        return entityPkList;
    }

    /**
     * parse List<PKValue> -> List<Number>
     * 
     * @param pkValueList
     * @return
     */
    public static List<? extends Number> parseNumberValueList(List<PKValue> pkValueList) {
        if (pkValueList == null) {
            return null;
        }

        List<Number> pkNumberList = Lists.newArrayListWithCapacity(pkValueList.size());

        for (PKValue pkValue : pkValueList) {
            pkNumberList.add(pkValue.getValueAsNumber());
        }

        return pkNumberList;
    }

    /**
     * parse List<Number> -> List<PKValue>
     * 
     * @param pkNumberList
     * @return
     */
    public static List<PKValue> parsePKValueList(List<? extends Number> pkNumberList) {
        if (pkNumberList == null) {
            return null;
        }

        List<PKValue> pkValueList = Lists.newArrayListWithCapacity(pkNumberList.size());

        for (Number pkNumber : pkNumberList) {
            pkValueList.add(PKValue.valueOf(pkNumber));
        }

        return pkValueList;
    }

    /**
     * parse Number[] -> PKValue[]
     * 
     * @param pkNumbers
     * @return
     */
    public static PKValue[] parsePKValueArray(Number[] pkNumbers) {
        if (pkNumbers == null) {
            return null;
        }

        PKValue[] pkValues = new PKValue[pkNumbers.length];

        for (int i = 0; i < pkNumbers.length; i++) {
            pkValues[i] = PKValue.valueOf(pkNumbers[i]);
        }

        return pkValues;
    }

    /**
     * parse PKValue[] -> Number[]
     * 
     * @param pkValues
     * @return
     */
    public static Number[] parseNumberArray(PKValue[] pkValues) {
        if (pkValues == null) {
            return null;
        }

        Number[] pkNumber = new Number[pkValues.length];

        for (int i = 0; i < pkValues.length; i++) {
            pkNumber[i] = pkValues[i].getValueAsNumber();
        }

        return pkNumber;
    }

}
