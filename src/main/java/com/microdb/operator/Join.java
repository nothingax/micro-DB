package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;

/**
 * 连接
 *
 * @author zhangjw
 * @version 1.0
 */
public class Join extends Operator{

    private final IOperatorIterator leftTableIterator;
    private final IOperatorIterator rightTableIterator;
    /**
     * join 条件
     */
    private final JoinPredicate joinPredicate;

    private Row left;
    private Row right;

    public Join(IOperatorIterator leftTableIterator, IOperatorIterator rightTableIterator, JoinPredicate joinPredicate) {
        this.leftTableIterator = leftTableIterator;
        this.rightTableIterator = rightTableIterator;
        this.joinPredicate = joinPredicate;
        this.left = null;
        this.right = null;
    }

    @Override
    public TableDesc getTableDesc() {
        return TableDesc.merge(leftTableIterator.getTableDesc(), rightTableIterator.getTableDesc());
    }

    @Override
    public void open() throws DbException {
        leftTableIterator.open();
        rightTableIterator.open();
        if (leftTableIterator.hasNext()) {
            left = leftTableIterator.next();
        }
        if (rightTableIterator.hasNext()) {
            right = rightTableIterator.next();
        }
        super.open();
    }

    @Override
    public void close() {
        super.close();
        leftTableIterator.close();
        rightTableIterator.close();
        left = null;
        right = null;
    }

    @Override
    protected Row fetchNextMatched() {
        while (left != null || right != null) {
            Row row = null;
            if (joinPredicate.filter(left, right)) {
                row = Row.merge(left, right);
            }

            // nested loop join
            if (rightTableIterator.hasNext()) {
                right = rightTableIterator.next();
            } else {
                if (leftTableIterator.hasNext()) {
                    left = leftTableIterator.next();
                    rightTableIterator.close();
                    rightTableIterator.open();
                    if (rightTableIterator.hasNext()) {
                        right = rightTableIterator.next();
                    }
                } else {
                    left = null;
                    right = null;
                }
            }

            if (row!=null) {
                return row;
            }
        }
        return null;
    }
}
