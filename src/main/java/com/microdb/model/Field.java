package com.microdb.model;

import com.microdb.model.enums.FieldType;

import java.io.Serializable;

/**
 * 分量/字段，表示元组中的一个属性值，类型见{@link com.microdb.model.enums.FieldType}
 *
 *
 * @author zhangjw
 * @version 1.0
 */
public interface Field extends Serializable {

    /**
     * 返回字段数据类型
     */
     FieldType getType();
}
