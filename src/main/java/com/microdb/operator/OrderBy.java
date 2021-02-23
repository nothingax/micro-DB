package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.field.Field;

import java.util.*;

/**
 * orderBy 操作符
 *
 * @author zhangjw
 * @version 1.0
 */
public class OrderBy implements IOperatorIterator {

    /**
     * 原始行数据的迭代器
     */
    private IOperatorIterator tableIterator;

    /**
     * 排序的字段
     */
    private int orderByFieldIndex;

    /**
     * 排序方式
     */
    private boolean asc;

    /**
     * 排序后的行数据
     */
    private List<Row> sortedRows = new ArrayList<>();

    /**
     * 排序后的行数据迭代器
     */
    private Iterator<Row> sortedRowIterator;

    private Row nextRow = null;
    private boolean isOpen = false;

    public OrderBy(IOperatorIterator tableIterator, int orderByFieldIndex, boolean asc) {
        this.tableIterator = tableIterator;
        this.orderByFieldIndex = orderByFieldIndex;
        this.asc = asc;
    }


    @Override
    public void open() throws DbException {
        tableIterator.open();
        while (tableIterator.hasNext()) {
            sortedRows.add(tableIterator.next());
        }

        sortedRows.sort(new RowComparator(orderByFieldIndex, asc));
        this.sortedRowIterator = sortedRows.iterator();
        this.isOpen = true;
    }


    @Override
    public boolean hasNext() throws DbException {
        if (!this.isOpen)
            throw new IllegalStateException("iterator not open");

        if (nextRow == null)
            nextRow = fetchNextRow();
        return nextRow != null;
    }

    @Override
    public Row next() throws DbException, NoSuchElementException {
        if (nextRow == null) {
            nextRow = fetchNextRow();
            if (nextRow == null)
                throw new NoSuchElementException();
        }
        Row result = nextRow;
        nextRow = null;
        return result;
    }

    private Row fetchNextRow() {
        if (sortedRowIterator.hasNext()) {
            return sortedRowIterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        this.isOpen = false;
        sortedRowIterator = null;
    }

    // ============================ 比较器 ==================================
    public static class RowComparator implements Comparator<Row> {
        int fieldIndex;
        boolean asc;

        public RowComparator(int fieldIndex, boolean asc) {
            this.fieldIndex = fieldIndex;
            this.asc = asc;
        }

        public int compare(Row o1, Row o2) {
            Field t1 = (o1).getField(fieldIndex);
            Field t2 = (o2).getField(fieldIndex);
            if (t1.compare(Predicate.OperationEnum.EQUALS, t2))
                return 0;
            if (t1.compare(Predicate.OperationEnum.GREATER_THAN, t2))
                return asc ? 1 : -1;
            else
                return asc ? -1 : 1;
        }

    }
}
