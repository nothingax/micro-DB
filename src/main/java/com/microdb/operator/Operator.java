package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;

import java.util.NoSuchElementException;

/**
 * 操作符基类，抽取各操作符的共有逻辑，提供各操作符需要的模板代码
 *
 * @author zhangjw
 * @version 1.0
 */
public abstract class Operator implements IOperatorIterator {

    /**
     * 缓存
     */
    private Row nextRow = null;
    private boolean isOpen = false;

    @Override
    public void open() throws DbException {
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
        this.nextRow = null;
        this.isOpen = false;
    }

    @Override
    public abstract TableDesc getTableDesc();

    protected abstract Row fetchNextMatched();
}
