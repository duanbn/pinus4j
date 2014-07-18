package com.pinus.cluster.beans;

import java.io.Serializable;

import com.pinus.util.StringUtils;

/**
 * 数据库索引bean.
 * 抽象一个数据库索引
 *
 * @author duanbn
 */
public class DBIndex implements Serializable {

    /**
     * 被索引的字段名.
     */
    private String field;

    /**
     * 是否是唯一索引
     */
    private boolean isUnique;

    /**
     * 生成索引名
     */
    public String getIndexName() {
        StringBuilder indexName = new StringBuilder();
        indexName.append("index__").append(StringUtils.removeBlank(field).replaceAll(",", "__"));
        return indexName.toString();
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((field == null) ? 0 : field.hashCode());
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
        DBIndex other = (DBIndex) obj;
        if (field == null) {
            if (other.field != null)
                return false;
        } else if (!field.equals(other.field))
            return false;
        if (isUnique != other.isUnique)
            return false;
        return true;
    }

}
