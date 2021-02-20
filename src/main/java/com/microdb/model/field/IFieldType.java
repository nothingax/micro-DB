package com.microdb.model.field;

/**
 * 字段类型
 *
 * @author zhangjw
 * @version 1.0
 */
public interface IFieldType {

    /**
     * 返回字段占用的字节数
     */
     int getSizeInByte();
}
