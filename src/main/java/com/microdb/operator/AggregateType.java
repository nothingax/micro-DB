package com.microdb.operator;

/**
 * 聚合方式枚举
 *
 * @author zhangjw
 * @version 1.0
 */
public enum AggregateType {
    MIN,
    MAX,
    SUM,
    AVG,
    COUNT
    ;

    static final int NO_GROUPING = -1;
}
