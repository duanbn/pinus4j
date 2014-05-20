package com.pinus.api.query;

/**
 * 数据库排序枚举.
 *
 * @author duanbn
 */
public enum Order {

    /**
     * 升序.
     */
    ASC("asc"),
    /**
     * 降序.
     */
    DESC("desc");

    private String value;

    private Order(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

}
