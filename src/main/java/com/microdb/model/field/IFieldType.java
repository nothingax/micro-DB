package com.microdb.model.field;

import java.io.DataInputStream;

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

    /**
     * 文件流中字节反序列化，解析出字段值
     */
    Field parse(DataInputStream dis);
}
