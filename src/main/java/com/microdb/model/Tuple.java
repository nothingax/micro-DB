package com.microdb.model;

import com.microdb.model.field.Field;

import java.io.Serializable;
import java.util.List;

/**
 * 元组，表中的一行
 * 一行中有多个属性，属性值由{@link Field}的实现类表示
 *
 * @author zhangjw
 * @version 1.0
 */
public class Tuple implements Serializable {
    private static final long serialVersionUID = 3508867799019762862L;

    private List<Field> fields;

    public void setField(int index, Field field) {
        fields.set(index, field);
    }

    public Field getField(int index) {
        return fields.get(index);
    }

}
