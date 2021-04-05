package com.microdb.model.table;

import com.microdb.model.field.FieldType;
import com.microdb.model.field.IFieldType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 表结构描述
 *
 * @author zhangjw
 * @version 1.0
 */
public class TableDesc implements Serializable {

    private static final long serialVersionUID = -1072772284817404997L;

    /**
     * 表中的属性
     */
    private final List<Attribute> attributes;

    public TableDesc(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    public TableDesc(FieldType... fieldTypes) {
        this.attributes = Stream.of(fieldTypes).map(x -> new Attribute(null, x)).collect(Collectors.toList());
    }

    public int getAttributesNum() {
        return attributes.size();
    }

    public List<FieldType> getFieldTypes() {
        return this.attributes.stream().map(Attribute::getFieldType).collect(Collectors.toList());
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    /**
     * 两个表结构合并成一个，在join时使用
     *
     * @param left  左表
     * @param right 右表
     * @return 合并后的新表结构
     */
    public static TableDesc merge(TableDesc left, TableDesc right) {
        List<Attribute> leftAttributes = left.getAttributes();
        List<Attribute> rightAttributes = right.getAttributes();
        ArrayList<Attribute> newTableAttrs = new ArrayList<>(leftAttributes.size() + rightAttributes.size());
        newTableAttrs.addAll(leftAttributes);
        newTableAttrs.addAll(rightAttributes);
        return new TableDesc(newTableAttrs);
    }

    /**
     * 返回一行数据占用的字节数
     */
    public int getRowMaxSizeInBytes() {
        return this.attributes
                .stream()
                .map(Attribute::getFieldType)
                .mapToInt(IFieldType::getSizeInByte)
                .sum();
    }

    public FieldType getFieldType(int keyFieldIndex) {
        return this.attributes.get(keyFieldIndex).getFieldType();
    }
    /**
     * 属性，表中的一列
     */
    public static class Attribute implements Serializable {

        private static final long serialVersionUID = 6568833840007706206L;
        /**
         * 字段名称
         */
        private final String filedName;

        /**
         * 字段类型
         */
        private final FieldType fieldType;

        public String getFiledName() {
            return filedName;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        public Attribute(String filedName, FieldType fieldType) {
            this.filedName = filedName;
            this.fieldType = fieldType;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Attribute)) {
                return false;
            }

            return fieldType.equals(((Attribute) obj).fieldType)
                    && Objects.equals(filedName, ((Attribute) obj).filedName);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == null ? this.attributes == null : ((TableDesc) obj).attributes.equals(this.attributes);
    }
}
