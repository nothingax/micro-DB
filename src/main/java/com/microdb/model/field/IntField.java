package com.microdb.model.field;

import com.microdb.operator.PredicateEnum;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * int 类型字段
 *
 * @author zhangjw
 * @version 1.0
 */
public class IntField implements Field, Serializable {

    private static final long serialVersionUID = -4714840292406579518L;

    private final int value;

    public IntField(int value) {
        this.value = value;
    }


    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    @Override
    public boolean compare(PredicateEnum predicateEnum, Field operand) {
        IntField operandValue = (IntField) operand;
        switch (predicateEnum) {
            case EQUALS:
                return value == operandValue.value;
            case NOT_EQUALS:
                return value != operandValue.value;

            case GREATER_THAN:
                return value > operandValue.value;

            case GREATER_THAN_OR_EQ:
                return value >= operandValue.value;

            case LESS_THAN:
                return value < operandValue.value;

            case LESS_THAN_OR_EQ:
                return value <= operandValue.value;
        }

        return false;
    }

    @Override
    public FieldType getType() {
        return FieldType.INT;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }
}
