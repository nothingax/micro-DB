package com.microdb.model.field;

import com.microdb.operator.PredicateEnum;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Long 类型字段
 *
 * @author zhangjw
 * @version 1.0
 */
public class LongField implements Field {
    private static final long serialVersionUID = 6463445416908424053L;

    private final long value;

    public LongField(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public FieldType getType() {
        return FieldType.LONG;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeLong(value);
    }

    @Override
    public boolean compare(PredicateEnum predicateEnum, Field operand) {
        LongField operandValue = (LongField) operand;
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
}
