package com.microdb.model;

import com.microdb.exception.DbException;
import com.microdb.model.dbfile.DbTableFile;

import java.io.IOException;
import java.util.HashMap;

/**
 * 数据库对象
 *
 * @author zhangjw
 * @version 1.0
 */
public class DataBase {
    /**
     * dbTableFile id to DbTable
     */
    private HashMap<Integer, DbTable> tables;
    private HashMap<String, DbTable> tableNameToTable;

    public DataBase() {
        this.tables = new HashMap<>();
        this.tableNameToTable = new HashMap<>();
    }

    /**
     * 添加表
     *
     * @param dbTableFile table file
     * @param tableName   table name
     */
    public void addTable(DbTableFile dbTableFile, String tableName, TableDesc tableDesc) {
        DbTable dbTable = new DbTable(tableName, dbTableFile, tableDesc);
        tables.put(dbTableFile.getId(), dbTable);
        tableNameToTable.put(tableName, dbTable);
    }

    public DbTable getDbTableByName(String name) {
        DbTable dbTable = tableNameToTable.get(name);
        if (dbTable == null) {
            throw new DbException("table not exist");
        }
        return dbTable;
    }

    public void insertRow(Tuple tuple, String tableName) throws IOException {
        this.getDbTableByName(tableName).insertRow(tuple);
    }
}