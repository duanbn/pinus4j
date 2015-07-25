package org.pinus4j.utils;

import java.util.List;

import org.pinus4j.entity.meta.PKValue;

import com.google.common.collect.Lists;

/**
 * @author shanwei Jul 25, 2015 9:42:36 AM
 */
public class PKUtil {

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
