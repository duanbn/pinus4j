package org.pinus4j.entity.meta;

import java.util.Arrays;

/**
 * 表示一个实体的主键
 * 
 * @author shanwei Jul 24, 2015 4:28:49 PM
 */
public class EntityPK {

    private PKName[]  pkNames;

    private PKValue[] pkValues;

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
