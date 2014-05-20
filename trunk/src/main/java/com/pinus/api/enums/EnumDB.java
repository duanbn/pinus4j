package com.pinus.api.enums;

/**
 * 考拉存储中间件支持的数据库类型.
 *
 * @author duanbn
 */
public enum EnumDB {

    MYSQL("com.mysql.jdbc.Driver");

    /**
     * 数据库驱动.
     */
    private String driverClass;

    private EnumDB(String driverClass) {
        this.driverClass = driverClass;
    }

    /**
     * 获取驱动.
     *
     * @return 驱动
     */
    public String getDriverClass() {
    	return this.driverClass;
    }

}

