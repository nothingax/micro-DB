package com.microdb.bufferpool;

import com.microdb.connection.Connection;
import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存池
 *
 * @author zhangjw
 * @version 1.0
 */
public class BufferPool {

    private ConcurrentHashMap<PageID, Page> pool;

    /**
     * 容量：最多缓存的页数
     */
    private int capacity;

    public BufferPool(int capacity) {
        this.capacity = capacity;
        pool = new ConcurrentHashMap<>();
    }


    /**
     * 首先从缓存中获取，
     * 缓存中没有时，从磁盘读取；从磁盘读取前先检查缓存是否满，已满则执行驱逐
     *
     * @param pageID pageID
     */
    public Page getPage(PageID pageID) {
        Page page = pool.get(pageID);
        if (page == null) {
            // HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
            // if (dirtyPages.containsKey(pageID)) {
            //     return dirtyPages.get(pageID);
            // }
            if (isFull()) {
                // 驱逐页面 TODO 设计驱逐策略
                this.evictPages();
            }
            page = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableFile().readPageFromDisk(pageID);
            pool.put(pageID, page);
        }
        return page;
    }

    /**
     * 缓存池是否已满
     */
    private boolean isFull() {
        return pool.size() >= capacity;
    }

    /**
     * 驱逐页面，不常使用的页面从缓冲区移除
     */
    private void evictPages() {
        for (Map.Entry<PageID, Page> entry : pool.entrySet()) {
            int tableId = entry.getKey().getTableId();
            Page page = entry.getValue();
            // 刷盘
            DataBase.getInstance().getDbTableById(tableId).getTableFile().writePageToDisk(page);
            // 移除缓存 // TODO 根据脏页状态来移除
            pool.remove(page.getPageID());
        }
    }

    public void insertRow(Row row, String tableName) throws IOException {
        DbTable dbTable = DataBase.getInstance().getDbTableByName(tableName);
        dbTable.insertRow(row);

        // 获取插入过程中产生的脏页
        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        if (dirtyPages.isEmpty()) {
            throw new DbException("插入数据应产生脏页脏页");
        }

        // TODO dirtyPages.size 极限值过大说明page size 配置不合理
        System.out.println("dirtyPages.size():"+dirtyPages.size());
        for (Map.Entry<PageID, Page> entry : dirtyPages.entrySet()) {
            PageID pageID = entry.getKey();
            if (isFull()) {
                evictPages();
            }
            pool.put(pageID, entry.getValue());
        }
        Connection.clearDirtyPages();
    }

    public void deleteRow(Row row) throws IOException {
        DbTable dbTable = null;
        try {
            dbTable = DataBase.getInstance().getDbTableById(row.getKeyItem().getPageID().getTableId());
        } catch (Exception e) {
            e.printStackTrace();
        }
        dbTable.deleteRow(row);

        // 所有脏页都放在了thread 里
        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        System.out.println("del.dirtyPages.size():"+dirtyPages.size());
        for (Map.Entry<PageID, Page> entry : dirtyPages.entrySet()) {
            PageID pageID = entry.getKey();
            if (isFull()) {
                evictPages();
            }
            pool.put(pageID, entry.getValue());
        }
        // 如果不清，会严重降低性能
        Connection.clearDirtyPages();
    }
}
