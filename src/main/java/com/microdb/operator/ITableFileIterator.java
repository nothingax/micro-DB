package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;

import java.util.NoSuchElementException;

/**
 * 表迭代器接口
 *
 * @author zhangjw
 * @version 1.0
 */
public interface ITableFileIterator {

    void open() throws DbException;

    boolean hasNext() throws DbException;

    Row next() throws DbException, NoSuchElementException;

    void close();
}
