package com.microdb.transaction;

import com.microdb.annotation.VisibleForTest;
import com.microdb.connection.Connection;
import com.microdb.exception.DbException;
import com.microdb.logging.RedoLogFile;
import com.microdb.model.DataBase;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.io.IOException;
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

    private DataBase dataBase;

    /**
     * 构造事务,事务ID自增
     */
    public Transaction(Lock.LockType lockType) {
        this.dataBase = DataBase.getInstance();
        this.transactionId = new TransactionID();
        this.lockType = lockType;
    }

    public void setDataBase(DataBase dataBase) {
        this.dataBase = dataBase;
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
        dataBase.getUndoLogFile().recordTxStart(transactionId);

        try {
            dataBase.getRedoLogFile().recordTxStart(transactionId);
        } catch (IOException e) {
            throw new DbException(" redo log recordTxStart error ", e);
        }
    }

    /**
     * 提交事务
     * 根据2PL协议，提交事务时需要释放锁
     * NO-STEAL/force策略，所以事务提交时需要将脏页刷盘，事务提交前要确保脏页不被刷盘。
     * <p>
     * STEAL/No-force策略，事务提交，脏页也可不刷盘
     */
    public void commit() {
        // NO-STEAL 事务中产生的脏页刷盘
        // List<PageID> pageIDs = DataBase.getLockManager().getPageIDs(transactionId);
        // DataBase.getBufferPool().flushPages(pageIDs, transactionId);

        // STEAL/No-force策略，事务提交，脏页也可不刷盘,需记录日志到redolog
        try {
            RedoLogFile redoLogFile = dataBase.getRedoLogFile();
            redoLogFile.recordTxCommit(transactionId);
            List<Page> pages = dataBase.getLockManager().getPages(transactionId);
            for (Page page : pages) {
                if (page.isDirty()) {
                    redoLogFile.recordPageChange(transactionId, page.getBeforePage(), page);
                }
            }
            redoLogFile.flush();
        } catch (IOException e) {
            throw new DbException("redo log recordTxCommit error", e);
        }
        List<PageID> pageIDs = dataBase.getLockManager().getPageIDs(transactionId);
        // 事务提交后更新页快照
        for (PageID pageID : pageIDs) {
            Page page = dataBase.getBufferPool().getPage(pageID);
            page.saveBeforePage();
        }

        // 释放锁
        dataBase.getLockManager().releaseLock(transactionId);

        // 清除ThreadLocal中的事务
        Connection.clearTransaction();
    }

    /**
     * 终止事务，并回滚事务中修改的的脏页
     * 由于NO-STEAL策略，事务提交前脏页都没有刷盘，直接丢弃缓冲池中的相关页即可
     */
    public void abort() {

        // NO-Steal/force策略
        // List<PageID> pageIDs = DataBase.getLockManager().getPageIDs(transactionId);
        // DataBase.getBufferPool().discardPages(pageIDs);

        // Steal/No-force策略
        // 将事务修改过的页面，在磁盘刷回原始版本，缓存中丢弃
        dataBase.getUndoLogFile().rollback(transactionId);

        // 释放锁
        dataBase.getLockManager().releaseLock(transactionId);

        // 清除ThreadLocal中的事务
        Connection.clearTransaction();
    }

    @Override
    public String toString() {
        return "[" +
                "transactionId=" + transactionId +
                ", lockType=" + lockType +
                ']';
    }
}

