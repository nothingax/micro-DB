package com.microdb.model.field;

import com.microdb.exception.ParseException;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * 字段数据类型
 *
 * @author zhangjw
 * @version 1.0
 */
public enum FieldType implements IFieldType {
    INT() {
        @Override
        public int getSizeInByte() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) {
            try {
                return new IntField(dis.readInt());
            } catch (IOException e) {
                throw new ParseException("parse field failed", e);
            }
        }
    },

    LONG() {
        @Override
        public int getSizeInByte() {
            return 8;
        }

        @Override
        public Field parse(DataInputStream dis) {
            try {
                return new LongField(dis.readLong());
            } catch (IOException e) {
                throw new ParseException("parse field failed", e);
            }
        }
    }
}
