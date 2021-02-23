package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;

import java.util.NoSuchElementException;

/**
 * filter 条件过滤操作
 *
 * @author zhangjw
 * @version 1.0
 */
public class Filter implements IOperatorIterator {

    /**
     * 条件谓词
     */
    private Predicate predicate;

    /**
     * 需要过滤的表中，行数据的迭代
     */
    private IOperatorIterator tableIterator;

    private Row nextRow = null;
    private boolean isOpen = false;

    public Filter(Predicate predicate, IOperatorIterator tableIterator) {
        this.predicate = predicate;
        this.tableIterator = tableIterator;
    }

    @Override
    public void open() throws DbException {
        tableIterator.open();
        this.isOpen = true;
    }

    @Override
    public boolean hasNext() throws DbException {
        if (!this.isOpen)
            throw new IllegalStateException("iterator not open");

        if (nextRow == null)
            nextRow = fetchNextMatched();
        return nextRow != null;
    }

    @Override
    public Row next() throws DbException, NoSuchElementException {
        if (nextRow == null) {
            nextRow = fetchNextMatched();
            if (nextRow == null)
                throw new NoSuchElementException();
        }
        Row result = nextRow;
        nextRow = null;
        return result;
    }

    @Override
    public void close() {
        this.isOpen = false;
        tableIterator.close();
        this.nextRow = null;
    }

    Row fetchNextMatched() throws NoSuchElementException, DbException {
        while (tableIterator.hasNext()) {
            Row nextRow = tableIterator.next();
            if (predicate.filter(nextRow)) {
                return nextRow;
            }
        }
        return null;
    }
}
