package org.pinus4j.entity.meta;


/**
 * 表示一个主键名
 * 
 * @author shanwei Jul 24, 2015 5:01:51 PM
 */
public class PKName {

    private String value;

    private PKName() {
    }

    public static PKName valueOf(String value) {
        PKName pkName = new PKName();
        pkName.setValue(value);
        return pkName;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        PKName other = (PKName) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

}
