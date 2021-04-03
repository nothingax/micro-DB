package com.microdb.model.table;

import com.microdb.exception.DbException;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.model.row.Row;

import java.io.IOException;
import java.util.Objects;

/**
 * 表对象
 *
 * @author zhangjw
 * @version 1.0
 */
public class DbTable {

    /**
     * table name
     */
    private final String tableName;

    /**
     * table id
     */
    private final int tableId;

    /**
     * table file
     */
    private final TableFile tableFile;

    /**
     * 表结构
     */
    private final TableDesc tableDesc;

    public DbTable(String tableName, TableFile tableFile, TableDesc tableDesc) {
        this.tableName = tableName;
        this.tableId = tableFile.getTableId();
        this.tableFile = tableFile;
        this.tableDesc = tableDesc;
    }

    public TableDesc getTableDesc() {
        return this.tableDesc;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableId() {
        return tableId;
    }

    public TableFile getTableFile() {
        return tableFile;
    }

    public void insertRow(Row row) throws IOException {
        if (row == null) {
            throw new DbException("insertRow error: row can not be null");
        }
        if (!Objects.equals(row.getTableDesc(), this.getTableDesc())) {
            throw new DbException("insertRow error: TableDesc not match");
        }
        if (row.getFields() == null || row.getFields().isEmpty()) {
            throw new DbException("insertRow error: have no fields value");
        }
        if (row.getFields().contains(null)) {
            throw new DbException("insertRow error: 'null' field is not support");
        }
        this.tableFile.insertRow(row);
    }

    public void deleteRow(Row row) throws IOException {
        if (!Objects.equals(row.getTableDesc(), this.getTableDesc())) {
            throw new DbException("insertRow error: TableDesc not match");
        }
        System.out.println("删除行" + row);
        this.tableFile.deleteRow(row);
    }

}
