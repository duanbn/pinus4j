package com.pinus.api.enums;

/**
 * 数据库主从选择枚举.
 * 每一个数据库集群可以有若干个从库，从库和主库的数据是实时同步的.
 * 目前先支持4个从库，如果有更多的从库可以添加此枚举的值即可.
 *
 * @author duanbn
 */
public enum EnumDBMasterSlave {

    /**
     * 主库
     */
    MASTER(0), 
    /**
     * 第一个从库.
     */
    SLAVE0(0),
    /**
     * 第二个从库.
     */
    SLAVE1(1),
    /**
     * 第三个从库.
     */
    SLAVE2(2),
    /**
     * 第四个从库.
     */
    SLAVE3(3);

    private int value;

    private EnumDBMasterSlave(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static EnumDBMasterSlave getEnum(int value) {
        for (EnumDBMasterSlave type : EnumDBMasterSlave.values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }

}
