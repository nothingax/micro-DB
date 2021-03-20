package com.microdb.operator;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.ITableFileIterator;
import com.microdb.model.dbfile.TableFile;

/**
 * 表顺序扫描 sequence scan，基于迭代器
 *
 * @author zhangjw
 * @version 1.0
 */
public class SeqScan extends Operator {

    private TableFile tableFile;

    /**
     * 表中行数据的迭代
     */
    private ITableFileIterator tableFileIterator;

    public SeqScan(int tableId) {
        this.tableFile = DataBase.getInstance().getDbTableById(tableId).getHeapTableFile();
        this.tableFileIterator = this.tableFile.getIterator();
    }

    @Override
    public void open() throws DbException {
        tableFileIterator.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        tableFileIterator.close();
    }

    @Override
    public TableDesc getTableDesc() {
        return tableFile.getTableDesc();
    }

    @Override
    protected Row fetchNextMatched() {
        if (tableFileIterator != null && tableFileIterator.hasNext()) {
            return tableFileIterator.next();
        }
        return null;
    }
}
