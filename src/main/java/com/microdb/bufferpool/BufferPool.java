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
     * 缓存中没有时，从磁盘读取，并放入缓存；从磁盘读取前先检查缓存是否满，已满则先执行驱逐
     *
     * @param pageID pageID
     */
    public Page getPage(PageID pageID) {
        Page page = pool.get(pageID);
        if (page == null) {
            if (isFull()) {
                // 驱逐页面
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
     * 驱逐页面，不常使用的页面从缓冲区移除，移除前将脏页刷盘
     * <p>
     * TODO 目前实现是对整个池中的页全部移除，后续优化根据使用频率驱逐页面:LFU，或使用其他缓存失效算法
     */
    private void evictPages() {
        for (Map.Entry<PageID, Page> entry : pool.entrySet()) {
            Page page = entry.getValue();

            // 刷盘
            if (page.isDirty()) {
                flushPage(page);
            }

            // 移除缓存
            pool.remove(page.getPageID());
        }
    }

    private void flushPage(Page page) {
        // 脏页刷盘
        int tableId = page.getPageID().getTableId();
        DataBase.getInstance().getDbTableById(tableId).getTableFile().writePageToDisk(page);
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
        System.out.println("dirtyPages.size():" + dirtyPages.size());
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
        DbTable dbTable = DataBase.getInstance().getDbTableById(row.getKeyItem().getPageID().getTableId());
        dbTable.deleteRow(row);
        // 所有脏页都放在了thread 里
        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        System.out.println("del.dirtyPages.size():" + dirtyPages.size());
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
