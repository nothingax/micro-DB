package com.microdb.model;

import com.microdb.exception.DbException;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.page.heap.Page;
import com.microdb.model.page.heap.PageID;

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
    private HashMap<Integer, DbTable> tableId2Table;
    private HashMap<String, DbTable> tableName2Table;

    private DataBase() {
        this.tableId2Table = new HashMap<>();
        this.tableName2Table = new HashMap<>();
    }

    public static DataBase getInstance() {
        return singleton.get();
    }

    /**
     * 添加表
     *
     * @param tableFile table file
     * @param tableName table name
     */
    public void addTable(TableFile tableFile, String tableName, TableDesc tableDesc) {
        DbTable dbTable = new DbTable(tableName, tableFile, tableDesc);
        tableId2Table.put(tableFile.getTableId(), dbTable);
        tableName2Table.put(tableName, dbTable);
    }

    public DbTable getDbTableById(int tableId) {
        DbTable dbTable = tableId2Table.get(tableId);
        if (dbTable == null) {
            throw new DbException("table not exist");
        }
        return dbTable;
    }

    public DbTable getDbTableByName(String name) {
        DbTable dbTable = tableName2Table.get(name);
        if (dbTable == null) {
            throw new DbException("table not exist");
        }
        return dbTable;
    }

    public Page getPage(PageID pageID) throws DbException {
        try {
            return this.getDbTableById(pageID.getTableId()).getHeapTableFile().readPageFromDisk(pageID);
        } catch (IOException e) {
            throw new DbException("read page failed", e);
        }
    }

    public void insertRow(Row row, String tableName) throws IOException {
        this.getDbTableByName(tableName).insertRow(row);
    }
}