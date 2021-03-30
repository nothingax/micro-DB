package com.microdb.bufferpool;

import com.microdb.connection.Connection;
import com.microdb.exception.DbException;
import com.microdb.exception.TransactionException;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import com.microdb.transaction.TransactionID;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * 读取页
     * 首先从缓存中获取，
     * 缓存中没有时，从磁盘读取，并放入缓存；从磁盘读取前先检查缓存是否满，已满则先执行驱逐
     * <p>
     * 实现2PL+页级锁：事务中的语句开始执行时加锁，getPage是最适合的加锁位置
     *
     * @param pageID pageID
     */
    public Page getPage(PageID pageID) {
        Transaction currentTransaction = Connection.currentTransaction();
        try {
            DataBase.getLockManager().acquireLock(currentTransaction, pageID);
        } catch (TransactionException e) {
            System.out.println(String.format("acquire lock 失败,transaction=%s,pageID=%s", currentTransaction, pageID));
            throw e;
        }

        System.out.println(String.format("访问页%s，获取锁成功", pageID));

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

    public void insertRow(Row row, String tableName) throws IOException {
        Transaction transaction = Connection.currentTransaction();
        if (!Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {
            throw new TransactionException("x lock must be granted in updating");
        }

        DbTable dbTable = DataBase.getInstance().getDbTableByName(tableName);
        dbTable.insertRow(row);

        // 获取插入过程中产生的脏页，放入缓存
        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        if (dirtyPages.isEmpty()) {
            throw new DbException("插入数据应产生脏页脏页");
        }
        // TODO dirtyPages.size 极限值过大说明page size 配置不合理
        System.out.println("dirtyPages.size():" + dirtyPages.size());
        for (Map.Entry<PageID, Page> entry : dirtyPages.entrySet()) {
            pool.put(entry.getKey(), entry.getValue());
        }
        Connection.clearDirtyPages();
    }

    public void deleteRow(Row row) throws IOException {
        Transaction transaction = Connection.currentTransaction();
        if (!Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {
            throw new TransactionException("x lock must be granted in updating");
        }
        DbTable dbTable = DataBase.getInstance().getDbTableById(row.getKeyItem().getPageID().getTableId());
        dbTable.deleteRow(row);

        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        System.out.println("del.dirtyPages.size():" + dirtyPages.size());
        for (Map.Entry<PageID, Page> entry : dirtyPages.entrySet()) {
            pool.put(entry.getKey(), entry.getValue());
        }
        // 如果不清，会严重降低性能
        Connection.clearDirtyPages();
    }

    /**
     * 缓存池是否已满
     */
    private boolean isFull() {
        return pool.size() >= capacity;
    }

    /**
     * 驱逐页面，不常使用的页面从缓冲区移除
     * <p>
     * TODO 目前实现是对整个池中的页全部移除，后续优化根据使用频率驱逐页面:LFU，或使用其他缓存失效算法
     * <p>
     * TODO NO-STEAL 策略实现，事务提交前不能将事务产生的脏页刷盘
     * 刷盘时机改为事务提交时, see {@link Transaction#commit()}
     */
    private void evictPages() {
        for (Map.Entry<PageID, Page> entry : pool.entrySet()) {
            Page page = entry.getValue();

            // 刷盘
            // NO-STEAL 事务提交前不能将事务产生的脏页刷盘
            // if (page.isDirty()) {
            //     flushPage(page);
            // }

            // 移除缓存
            pool.remove(page.getPageID());
        }
    }

    /**
     * 刷盘
     */
    private void flushPage(Page page) {
        int tableId = page.getPageID().getTableId();
        DataBase.getInstance().getDbTableById(tableId).getTableFile().writePageToDisk(page);

        // TODO 加入缓存失效算法后，需要设置为非脏页
    }

    public void flushPages(List<PageID> pageIDs, TransactionID transactionId) {
        if (Objects.isNull(pageIDs) || pageIDs.isEmpty()) {
            System.out.println("flushPages : pageIDs is null or empty ");
            return;
        }
        for (PageID pageID : pageIDs) {
            Page page = pool.get(pageID);
            if (page.isDirty()) {
                page.markDirty(transactionId);
                pool.put(pageID, page);
                flushPage(page);
            }
        }
    }

    /**
     * 丢弃脏页
     */
    public void discardPages(List<PageID> pageIDs) {
        if (Objects.isNull(pageIDs) || pageIDs.isEmpty()) {
            System.out.println("discardPages : pageIDs is null or empty ");
            return;
        }
        for (PageID pageID : pageIDs) {
            Page page = pool.get(pageID);
            if (page.isDirty()) {
                pool.remove(pageID);
            }
        }
    }
}
