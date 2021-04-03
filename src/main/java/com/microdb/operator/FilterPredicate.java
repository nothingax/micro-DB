package com.microdb.operator;

import com.microdb.model.row.Row;
import com.microdb.model.field.Field;

/**
 * Predicate 用于条件过滤,见{@link Filter}
 *
 * @author zhangjw
 * @version 1.0
 */
public class FilterPredicate {

    /**
     * row中字段{@link Row#fields}的下标
     */
    private int fieldIndex;
    /**
     * 操作符
     */
    private PredicateEnum predicateEnum;

    /**
     * 参数操作数
     */
    private Field paramOperand;


    public FilterPredicate(int fieldIndex, PredicateEnum predicateEnum, Field paramOperand) {
        this.fieldIndex = fieldIndex;
        this.predicateEnum = predicateEnum;
        this.paramOperand = paramOperand;
    }

    public boolean filter(Row row) {
        return row.getField(fieldIndex).compare(predicateEnum, paramOperand);
    }


}
