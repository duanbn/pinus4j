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
import java.util.List;

import org.pinus4j.utils.StringUtil;

/**
 * 数据库索引bean. 抽象一个数据库索引
 *
 * @author duanbn
 */
public class DBTableIndex implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 被索引的字段名.
     */
    private List<String>      fields;

    /**
     * 是否是唯一索引
     */
    private boolean           isUnique;

    /**
     * 生成索引名
     */
    public String getIndexName() {
        StringBuilder indexName = new StringBuilder();
        StringBuilder fieldPhrase = new StringBuilder();
        for (String field : fields) {
            fieldPhrase.append(field).append("_");
        }
        fieldPhrase.deleteCharAt(fieldPhrase.length() - 1);
        indexName.append("index__").append(fieldPhrase.toString());
        return indexName.toString();
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    @Override
    public String toString() {
        return "DBTableIndex [fields=" + fields + ", isUnique=" + isUnique + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + (isUnique ? 1231 : 1237);
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
        DBTableIndex other = (DBTableIndex) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (isUnique != other.isUnique)
            return false;
        return true;
    }

}
