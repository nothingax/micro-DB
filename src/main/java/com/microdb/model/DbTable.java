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
    private String name;

    /**
     * table file
     */
    private DbTableFile dbTableFile;


    public DbTable(String name, DbTableFile dbTableFile) {
        this.name = name;
        this.dbTableFile = dbTableFile;
    }
}
