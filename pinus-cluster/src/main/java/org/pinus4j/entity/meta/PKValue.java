package org.pinus4j.entity.meta;

/**
 * 表示一个主键值
 * 
 * @author shanwei Jul 24, 2015 4:29:02 PM
 */
public class PKValue {

    private Object pkValue;

    private PKValue(Object pkValue) {
        this.pkValue = pkValue;
    }

    public static PKValue valueOf(Object pkValue) {
        PKValue pk = new PKValue(pkValue);
        return pk;
    }

    public void setPkValue(Object pkValue) {
        this.pkValue = pkValue;
    }

    public Object getPkValue() {
        return pkValue;
    }

    public String getValueAsString() {
        return this.pkValue.toString();
    }

    public Number getValueAsNumber() {
        if (this.pkValue instanceof Number) {
            return (Number) this.pkValue;
        } else {
            throw new ClassCastException("value is not Number type");
        }
    }

    public int getValueAsInt() {
        if (this.pkValue instanceof Number) {
            return ((Number) this.pkValue).intValue();
        } else {
            throw new ClassCastException("value is not Number type");
        }
    }

    public long getValueAsLong() {
        if (this.pkValue instanceof Number) {
            return ((Number) this.pkValue).longValue();
        } else {
            throw new ClassCastException("value is not Number type");
        }
    }

    @Override
    public String toString() {
        return "PKValue [pkValue=" + pkValue + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pkValue == null) ? 0 : pkValue.hashCode());
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
        PKValue other = (PKValue) obj;
        if (pkValue == null) {
            if (other.pkValue != null)
                return false;
        } else if (!pkValue.equals(other.pkValue))
            return false;
        return true;
    }

}
