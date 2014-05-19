package com.pinus.api.query;

/**
 * 查询条件操作符枚举.
 *
 * @author duanbn
 *
 * @see Condition
 */
public enum QueryOpt {

    /**
     * 等于.
     */
    EQ("="),
    /**
     * 不等于.
     */
    NOTEQ("<>"),
    /**
     * 大于.
     */
    GT(">"),
    /**
     * 大于等于.
     */
    GTE(">="),
    /**
     * 小于.
     */
    LT("<"),
    /**
     * 小于等于.
     */
    LTE("<="),
    /**
     * in查询.
     */
    IN("in"),
    /**
     * like查询.
     */
    LIKE("like");

    /**
     * 操作符
     */
    private String symbol;

    private QueryOpt(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return this.symbol;
    }

}
