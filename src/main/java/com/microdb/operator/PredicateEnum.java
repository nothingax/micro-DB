package com.microdb.operator;

/**
 * predicate 谓词，用于filter条件、join条件
 *
 * @author zhangjw
 * @version 1.0
 */
public enum PredicateEnum {
    EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, NOT_EQUALS, LIKE, LEFT_LIKE, RIGHT_LIKE;
}
