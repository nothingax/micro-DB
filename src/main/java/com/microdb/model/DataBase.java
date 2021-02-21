package com.microdb.model;

import com.microdb.exception.DbException;
import com.microdb.model.dbfile.DbTableFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 数据库对象
 *
 * @author zhangjw
 * @version 1.0
 */
public class DataBase {
    private static AtomicReference<DataBase> singleton = new AtomicReference<DataBase>(new DataBase());

    /**
     * dbTableFile id to DbTable
     */
    private HashMap<Integer, DbTable> tableIdToTable;
    private HashMap<String, DbTable> tableNameToTable;

    private DataBase() {
        this.tableIdToTable = new HashMap<>();
        this.tableNameToTable = new HashMap<>();
    }

    public static DataBase getInstance() {
        return singleton.get();
    }

    /**
     * 添加表
     *
     * @param dbTableFile table file
     * @param tableName   table name
     */
    public void addTable(DbTableFile dbTableFile, String tableName, TableDesc tableDesc) {
        DbTable dbTable = new DbTable(tableName, dbTableFile, tableDesc);
        tableIdToTable.put(dbTableFile.getId(), dbTable);
        tableNameToTable.put(tableName, dbTable);
    }

    public DbTable getDbTableById(int tableId) {
        DbTable dbTable = tableIdToTable.get(tableId);
        if (dbTable == null) {
            throw new DbException("table not exist");
        }
        return dbTable;
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