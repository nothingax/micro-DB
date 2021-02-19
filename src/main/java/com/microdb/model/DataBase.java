package com.microdb.model;

import com.microdb.model.dbfile.DbTableFile;

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

    public DataBase() {
        this.tables = new HashMap<>();
    }

    /**
     * 添加表
     *
     * @param dbTableFile table file
     * @param tableName   table name
     */
    public void addTable(DbTableFile dbTableFile, String tableName, TableDesc tableDesc) {
        tables.put(dbTableFile.getId(), new DbTable(tableName, dbTableFile, tableDesc));
    }
}
