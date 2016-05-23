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

package org.pinus4j.api.query.impl;


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
    LIKE("like"),
    /**
     * is null
     */
    ISNULL("is null"),
    /**
     * is not null
     */
    ISNOTNULL("is not null");

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
