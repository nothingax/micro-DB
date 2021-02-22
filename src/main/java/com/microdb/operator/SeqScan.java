package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.dbfile.TableFile;

import java.util.NoSuchElementException;

/**
 * 表顺序扫描 sequence scan，基于迭代器
 *
 * @author zhangjw
 * @version 1.0
 */
public class SeqScan implements IOperatorIterator {

    private TableFile tableFile;
    private ITableFileIterator tableFileIterator;

    public SeqScan(int tableId) {
        this.tableFile = DataBase.getInstance().getDbTableById(tableId).getTableFile();
        this.tableFileIterator = this.tableFile.getIterator();
    }

    @Override
    public void open() throws DbException {
        tableFileIterator.open();
    }

    @Override
    public boolean hasNext() throws DbException {
        return tableFileIterator.hasNext();
    }

    @Override
    public Row next() throws DbException, NoSuchElementException {
        return tableFileIterator.next();
    }

    @Override
    public void close() {
        tableFileIterator.close();
    }
}
