package com.microdb.operator;

import com.microdb.model.Row;
import com.microdb.model.field.Field;

/**
 * Predicate 用于条件过滤,见{@link Filter}
 *
 * @author zhangjw
 * @version 1.0
 */
public class Predicate {

    /**
     * row中字段{@link Row#fields}的下标
     */
    private int fieldIndex;
    /**
     * 操作符
     */
    private OperationEnum operationEnum;

    /**
     * 参数操作数
     */
    private Field paramOperand;


    public Predicate(int fieldIndex, OperationEnum operationEnum, Field paramOperand) {
        this.fieldIndex = fieldIndex;
        this.operationEnum = operationEnum;
        this.paramOperand = paramOperand;
    }

    public boolean filter(Row row) {
        return row.getField(fieldIndex).compare(operationEnum, paramOperand);
    }

    public enum OperationEnum {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, NOT_EQUALS, LIKE, LEFT_LIKE, RIGHT_LIKE;
    }

}
