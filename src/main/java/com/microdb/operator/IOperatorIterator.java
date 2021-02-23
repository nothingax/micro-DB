package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;

import java.util.NoSuchElementException;

/**
 * 迭代器接口，行数据的迭代
 *
 * @author zhangjw
 * @version 1.0
 */
public interface IOperatorIterator {

    void open() throws DbException;

    boolean hasNext() throws DbException;

    Row next() throws DbException, NoSuchElementException;

    void close();

    /**
     * 返回表结构
     */
    TableDesc getTableDesc();
}
