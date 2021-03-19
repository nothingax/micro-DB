package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;

/**
 * 删除
 *
 * @author zhangjw
 * @version 1.0
 */
public class Delete extends Operator {

    /**
     * 构造函数传入，需要删除的数据的迭代器
     */
    private IOperatorIterator tableIterator;


    public Delete(IOperatorIterator tableIterator) {
        this.tableIterator = tableIterator;
    }

    @Override
    public TableDesc getTableDesc() {
        return null;
    }

    /**
     * 删除符合条件的row
     */
    @Override
    protected Row fetchNextMatched() {
        // 删除的行数
        int count = 0;
        while (tableIterator.hasNext()) {
            Row row = tableIterator.next();
            DbTable table = DataBase.getInstance().getDbTableById(row.getKeyItem().getPageID().getTableId());
            try {
                table.deleteRow(row);
            } catch (Exception e) {
                System.out.println(row);
                throw new DbException("deleteRow 异常", e);
            }
            count += 1;
        }
        return null;
    }

    @Override
    public void open() throws DbException {
        super.open();
        tableIterator.open();
    }

    @Override
    public boolean hasNext() throws DbException {
        return super.hasNext();
    }

    @Override
    public void close() {
        tableIterator.close();
        super.close();
    }
}
