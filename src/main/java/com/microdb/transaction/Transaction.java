package com.microdb.transaction;

import com.microdb.annotation.VisibleForTest;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.page.PageID;

import java.util.List;

/**
 * 事务
 *
 * @author zhangjw
 * @version 1.0
 */
public class Transaction {
    /**
     * 事务ID对象
     */
    private TransactionID transactionId;

    /**
     * 事务使用的锁
     */
    private Lock.LockType lockType;

    private Transaction() {
    }

    /**
     * 构造事务,事务ID自增
     */
    public Transaction(Lock.LockType lockType) {
        this.transactionId = new TransactionID();
        this.lockType = lockType;
    }

    public TransactionID getTransactionId() {
        return transactionId;
    }

    public Lock.LockType getLockType() {
        return lockType;
    }

    @VisibleForTest
    public void setLockType(Lock.LockType lockType) {
        this.lockType = lockType;
    }

    public void start() {

    }

    /**
     * 提交事务
     * 根据2PL协议，提交事务时需要释放锁
     * 由于实现的是NO-STEAL策略，所以事务提交时需要将脏页刷盘，事务提交前要确保脏页不被刷盘。
     */
    public void commit() {
        // DataBase.getBufferPool().transactionComplete();

        // NO-STEAL 事务中产生的脏页刷盘
        List<PageID> pageIDs = DataBase.getLockManager().getPageIDs(transactionId);
        DataBase.getBufferPool().flushPages(pageIDs);

        // 释放锁
        DataBase.getLockManager().releaseLock(transactionId);

        // 清除ThreadLocal中的事务
        Connection.clearTransactionID();
    }

    public void abort() {

    }

    @Override
    public String toString() {
        return "[" +
                "transactionId=" + transactionId +
                ", lockType=" + lockType +
                ']';
    }
}

