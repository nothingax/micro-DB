package com.microdb.operator.bptree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.BPTreeTableFile;
import com.microdb.model.table.tablefile.ITableFileIterator;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.operator.Operator;

/**
 * B+Tree表扫描
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeScan extends Operator {
    private final TableFile tableFile;
    private final IndexPredicate indexPredicate;
    private final TableDesc tableDesc;
    /**
     * 表中行数据的迭代
     */
    private final ITableFileIterator tableFileIterator;

    public BPTreeScan(int tableId, IndexPredicate indexPredicate) {
        this.tableFile = DataBase.getInstance().getDbTableById(tableId).getTableFile();
        this.indexPredicate = indexPredicate;
        tableDesc = tableFile.getTableDesc();
        if (this.indexPredicate == null) {
            this.tableFileIterator = tableFile.getIterator();
        } else {
            this.tableFileIterator = ((BPTreeTableFile) tableFile).getIndexIterator(indexPredicate);
        }
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
        return tableDesc;
    }

    @Override
    protected Row fetchNextMatched() {
        if (tableFileIterator != null && tableFileIterator.hasNext()) {
            return tableFileIterator.next();
        }
        return null;
    }
}
