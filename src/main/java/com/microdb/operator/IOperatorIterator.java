package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;

import java.util.NoSuchElementException;

/**
 * 操作符迭代器接口
 *
 * @author zhangjw
 * @version 1.0
 */
public interface IOperatorIterator {

    void open() throws DbException;

    boolean hasNext() throws DbException;

    Row next() throws DbException, NoSuchElementException;

    void close();
}
