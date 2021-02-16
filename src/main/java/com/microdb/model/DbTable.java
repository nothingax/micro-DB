package com.microdb.model;

import com.microdb.model.dbfile.DbTableFile;

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
     * table file
     */
    private DbTableFile dbTableFile;

    /**
     * 表结构
     */
    private TableDesc tableDesc;

    public DbTable(String tableName, DbTableFile dbTableFile, TableDesc tableDesc) {
        this.tableName = tableName;
        this.dbTableFile = dbTableFile;
        this.tableDesc = tableDesc;
    }

    public TableDesc getTupleDesc() {
        return this.tableDesc;
    }

    public String getTableName() {
        return tableName;
    }

    public DbTableFile getDbTableFile() {
        return dbTableFile;
    }
}
