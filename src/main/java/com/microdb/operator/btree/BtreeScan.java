package com.microdb.operator.btree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.BTreeFile;
import com.microdb.model.dbfile.ITableFileIterator;
import com.microdb.model.dbfile.TableFile;
import com.microdb.operator.Operator;

/**
 * B+Tree表扫描
 *
 * @author zhangjw
 * @version 1.0
 */
public class BtreeScan extends Operator {
    private TableFile tableFile;
    private IndexPredicate indexPredicate;
    private TableDesc tableDesc;
    /**
     * 表中行数据的迭代
     */
    private ITableFileIterator tableFileIterator;

    public BtreeScan(int tableId, IndexPredicate indexPredicate) {
        this.tableFile = DataBase.getInstance().getDbTableById(tableId).getTableFile();
        this.indexPredicate = indexPredicate;
        tableDesc = tableFile.getTableDesc();
        if (this.indexPredicate == null) {
            this.tableFileIterator = tableFile.getIterator();
        } else {
            this.tableFileIterator = ((BTreeFile) tableFile).getIndexIterator(indexPredicate);
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
