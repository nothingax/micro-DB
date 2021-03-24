package com.microdb.model;

import com.microdb.bufferpool.BufferPool;
import com.microdb.exception.DbException;
import com.microdb.model.dbfile.TableFile;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 数据库对象
 *
 * @author zhangjw
 * @version 1.0
 */
public class DataBase {
    private static AtomicReference<DataBase> singleton = new AtomicReference<>(new DataBase());

    /**
     * 缓存池容量，单位：页
     */
    private int bufferPoolCapacity = 100;

    /**
     * dbTableFile id to DbTable
     */
    private HashMap<Integer, DbTable> tableId2Table;
    private HashMap<String, DbTable> tableName2Table;

    private BufferPool bufferPool;

    private DataBase() {
        this.tableId2Table = new HashMap<>();
        this.tableName2Table = new HashMap<>();
        this.bufferPool = new BufferPool(100);
    }

    public static DataBase getInstance() {
        return singleton.get();
    }

    public static BufferPool getBufferPool() {
        return singleton.get().bufferPool;
    }

    /**
     * 添加表
     *
     * @param tableFile table file
     * @param tableName table name
     */
    @Deprecated
    public void addTable(TableFile tableFile, String tableName, TableDesc tableDesc) {
        DbTable dbTable = new DbTable(tableName, tableFile, tableDesc);
        tableId2Table.put(tableFile.getTableId(), dbTable);
        tableName2Table.put(tableName, dbTable);
    }

    /**
     * 添加表
     *
     * @param tableFile table file
     * @param tableName table name
     */
    public void addTable(TableFile tableFile, String tableName) {
        TableDesc tableDesc = tableFile.getTableDesc();
        if (tableDesc == null) {
            throw new DbException("table file's table desc must not be null");
        }

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
}