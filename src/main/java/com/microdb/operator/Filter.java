package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;

import java.util.NoSuchElementException;

/**
 * filter 条件过滤操作
 *
 * @author zhangjw
 * @version 1.0
 */
public class Filter extends Operator {

    /**
     * 条件谓词
     */
    private Predicate predicate;

    /**
     * 需要过滤的表中，行数据的迭代
     */
    private IOperatorIterator tableIterator;

    public Filter(Predicate predicate, IOperatorIterator tableIterator) {
        this.predicate = predicate;
        this.tableIterator = tableIterator;
    }

    @Override
    public void open() throws DbException {
        tableIterator.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        tableIterator = null;
    }

    @Override
    public TableDesc getTableDesc() {
        return tableIterator.getTableDesc();
    }

    @Override
    public Row fetchNextMatched() throws NoSuchElementException, DbException {
        while (tableIterator != null && tableIterator.hasNext()) {
            Row nextRow = tableIterator.next();
            if (predicate.filter(nextRow)) {
                return nextRow;
            }
        }
        return null;
    }
}
