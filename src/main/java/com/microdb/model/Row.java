package com.microdb.model;

import com.microdb.model.field.Field;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 元组，表中的一行
 * 一行中有多个属性，属性值由{@link Field}的实现类表示
 *
 * @author zhangjw
 * @version 1.0
 */
public class Row implements Serializable {
    private static final long serialVersionUID = 3508867799019762862L;

    /**
     * 行中的字段
     */
    private Field[] fields;

    private TableDesc tableDesc;

    public Row(TableDesc tableDesc) {
        this.tableDesc = tableDesc;
        this.fields = new Field[tableDesc.getAttributesNum()];
    }

    public TableDesc getTableDesc() {
        return tableDesc;
    }

    public void setTableDesc(TableDesc tableDesc) {
        this.tableDesc = tableDesc;
    }

    public void setField(int index, Field field) {
        fields[index] = field;
    }

    public Field getField(int index) {
        return fields[index];
    }

    public void setFields(Field[] fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        StringBuilder column = new StringBuilder();
        for (int i = 0; i < fields.length - 1; i++) {
            column.append(fields[i].toString()).append("\t");
        }
        column.append(fields[fields.length - 1]).append("\n");
        return column.toString();
    }

    public List<Field> getFields() {
        return Stream.of(fields).collect(Collectors.toList());
    }
}
