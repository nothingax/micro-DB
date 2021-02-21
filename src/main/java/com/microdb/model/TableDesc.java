package com.microdb.model;

import com.microdb.model.field.FieldType;
import com.microdb.model.field.IFieldType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
    private List<Attribute> attributes;

    public TableDesc(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    public int getAttributesNum() {
        return attributes.size();
    }

    public List<FieldType> getFieldTypes() {
        return this.attributes.stream().map(Attribute::getFieldType).collect(Collectors.toList());
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

    /**
     * 属性，表中的一列
     */
    public static class Attribute implements Serializable {

        private static final long serialVersionUID = 6568833840007706206L;
        /**
         * 字段名称
         */
        private String filedName;

        /**
         * 字段类型
         */
        private FieldType fieldType;

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
