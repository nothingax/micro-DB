package com.microdb.operator;

import com.microdb.model.Row;

/**
 * join predicate, 两表的join条件
 *
 * @author zhangjw
 * @version 1.0
 */
public class JoinPredicate {
    private int fieldIndex1;
    private int fieldIndex2;
    private  PredicateEnum predicateEnum;

    public JoinPredicate(int fieldIndex1, int fieldIndex2, PredicateEnum predicateEnum) {
        this.fieldIndex1 = fieldIndex1;
        this.fieldIndex2 = fieldIndex2;
        this.predicateEnum = predicateEnum;
    }


    public boolean filter(Row o1, Row o2) {
        return o1.getField(fieldIndex1).compare(predicateEnum, o2.getField(fieldIndex2));
    }
}
