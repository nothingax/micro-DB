package com.microdb.transaction;

import com.microdb.exception.TransactionException;
import com.microdb.model.page.PageID;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁管理器：记录事务持有哪些页锁、页锁被哪些事务持有
 * 事务的ACID特性需要锁来支持
 * 锁的粒度为页级别
 *
 * @author zhangjw
 * @version 1.0
 */
public class LockManager {


    /**
     * 管理中的锁
     */
    private ConcurrentHashMap<PageID, Lock> lockTable;

    /**
     * 管理中的事务
     */
    private ConcurrentHashMap<TransactionID, List<PageID>> transactionTable;


    public LockManager() {
        lockTable = new ConcurrentHashMap<>();
        transactionTable = new ConcurrentHashMap<>();
    }

    /**
     * 获取页锁
     * 线程安全
     */
    public synchronized void acquireLock(TransactionID transactionID, PageID pageID)
            throws TransactionException {

        System.out.println(String.format("页:%s开始获取锁--，transactionID=%s", pageID, transactionID));
        // TODO

    }

    /**
     * 释放页锁
     * 线程安全
     */
    public synchronized void releaseLock(TransactionID transactionID)
            throws TransactionException {

        System.out.println(String.format("transactionID:%s释放锁", transactionID));

    }

    public List<PageID> getPageIDs(TransactionID transactionId) {
        return transactionTable.get(transactionId);
    }
}
