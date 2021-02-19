package com.microdb.model.field;

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
    public FieldType getType() {
        return FieldType.INT;
    }
}
