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

    private HashMap<Integer, DbTable> tables;

    public DataBase() {
        this.tables = new HashMap<>();
    }

    /**
     * 添加表
     *
     * @param dbTableFile table file
     * @param name talbe name
     */
    public void addTable(DbTableFile dbTableFile, String name,TableDesc tableDesc) {
        tables.put(dbTableFile.getId(), new DbTable(name, dbTableFile, tableDesc));
    }
}
