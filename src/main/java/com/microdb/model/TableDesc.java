package com.microdb.model;

import com.microdb.model.enums.FieldType;

import java.io.Serializable;
import java.util.List;

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

    /**
     * 属性，表中的一列
     */
    public static class Attribute implements Serializable{

        private static final long serialVersionUID = 6568833840007706206L;
        /**
         * 字段名称
         */
        private String filedName;

        /**
         * 字段类型
         */
        private FieldType fieldType;

        public Attribute(String filedName, FieldType fieldType) {
            this.filedName = filedName;
            this.fieldType = fieldType;
        }
    }

    public TableDesc(List<Attribute> attributes) {
        this.attributes = attributes;
    }
}
