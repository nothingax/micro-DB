package com.microdb.model;

import com.microdb.exception.DbException;
import com.microdb.model.dbfile.TableFile;

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
    private String tableName;

    /**
     * table id
     */
    private int tableId;

    /**
     * table file
     */
    private TableFile tableFile;

    /**
     * 表结构
     */
    private TableDesc tableDesc;

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
        if (!Objects.equals(row.getTableDesc(), this.getTableDesc())) {
            throw new DbException("insertRow error: TableDesc not match");
        }
        this.tableFile.insertRow(row);
    }
}
