package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;

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
    private final FilterPredicate filterPredicate;

    /**
     * 需要过滤的表中，行数据的迭代
     */
    private IOperatorIterator tableIterator;

    public Filter(FilterPredicate filterPredicate, IOperatorIterator tableIterator) {
        this.filterPredicate = filterPredicate;
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
            if (filterPredicate.filter(nextRow)) {
                return nextRow;
            }
        }
        return null;
    }
}
