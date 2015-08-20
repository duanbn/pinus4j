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

/**
 * 表示一个主键值
 * 
 * @author shanwei Jul 24, 2015 4:29:02 PM
 */
public class PKValue implements Serializable {

    private Object value;

    private PKValue(Object value) {
        this.value = value;
    }

    public static PKValue valueOf(Object value) {
        PKValue pk = new PKValue(value);
        return pk;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getValueAsString() {
        return this.value.toString();
    }

    public Number getValueAsNumber() {
        if (this.value instanceof Number) {
            return (Number) this.value;
        } else {
            throw new ClassCastException("value is not Number type");
        }
    }

    public int getValueAsInt() {
        if (this.value instanceof Number) {
            return ((Number) this.value).intValue();
        } else {
            throw new ClassCastException("value is not Number type");
        }
    }

    public long getValueAsLong() {
        if (this.value instanceof Number) {
            return ((Number) this.value).longValue();
        } else {
            throw new ClassCastException("value is not Number type");
        }
    }

    @Override
    public String toString() {
        return "PKValue [value=" + value + "]";
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
        PKValue other = (PKValue) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

}
