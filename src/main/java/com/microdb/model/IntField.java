package com.microdb.model;

import com.microdb.model.enums.FieldType;

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
