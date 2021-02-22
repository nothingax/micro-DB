package com.microdb.model;

import com.microdb.exception.DbException;
import com.microdb.model.dbfile.DbTableFile;

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
    private DbTableFile dbTableFile;

    /**
     * 表结构
     */
    private TableDesc tableDesc;

    public DbTable(String tableName, DbTableFile dbTableFile, TableDesc tableDesc) {
        this.tableName = tableName;
        this.tableId = dbTableFile.getId();
        this.dbTableFile = dbTableFile;
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

    public DbTableFile getDbTableFile() {
        return dbTableFile;
    }

    public void insertRow(Row row) throws IOException {
        if (!Objects.equals(row.getTableDesc(), this.getTableDesc())) {
            throw new DbException("insertRow error: TableDesc not match");
        }
        this.dbTableFile.insertRow(row);
    }
}
