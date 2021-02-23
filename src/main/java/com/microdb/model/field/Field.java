package com.microdb.model.field;

import com.microdb.operator.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * 分量/字段，表示元组中的一个属性值，类型见{@link FieldType}
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

    void serialize(DataOutputStream dos) throws IOException;

    String toString();

    boolean compare(Predicate.OperationEnum operationEnum, Field operand);
}
