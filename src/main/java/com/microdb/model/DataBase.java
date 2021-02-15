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

    public void addTable(DbTableFile dbTableFile, String name) {
        tables.put(dbTableFile.getId(), new DbTable(name, dbTableFile));
    }
}
