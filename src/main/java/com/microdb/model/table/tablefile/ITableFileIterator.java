package com.microdb.model.table.tablefile;

import com.microdb.exception.DbException;
import com.microdb.model.row.Row;

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
