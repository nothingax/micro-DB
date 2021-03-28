package com.microdb.transaction;

import com.microdb.model.page.PageID;

import java.util.HashSet;
import java.util.Set;

/**
 * 锁
 * 支持独占锁、共享锁
 *
 * @author zhangjw
 * @version 1.0
 */
public class Lock {
    /**
     * 锁类型枚举
     */
    public enum LockType {
        /**
         * 共享锁
         */
        SLock,

        /**
         * 独占锁
         */
        XLock
    }

    /**
     * 被锁的页
     */
    private PageID pageID;

    /**
     * 锁类型
     */
    private LockType lockType;

    // /**
    //  * 持有该锁的事务,独占锁：一个页只能被一个事务持有
    //  * 后续实现共享锁(s lock)
    //  */
    // private TransactionID lockHolder;

    /**
     * 持有该锁的事务
     */
    private Set<TransactionID> lockHolders;

    public Lock(PageID pageID, LockType lockType) {
        this.pageID = pageID;
        this.lockType = lockType;
        lockHolders = new HashSet<>();
    }
    //
    // public Lock(PageID pageID) {
    //     this.pageID = pageID;
    //     lockHolders = new ArrayList<>();
    // }
    //
    // public TransactionID getLockHolder() {
    //     return lockHolder;
    // }


    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    public LockType getLockType() {
        return lockType;
    }

    /**
     * 添加持有锁的事务
     */
    public void addHolder(TransactionID transactionID) {
        lockHolders.add(transactionID);
    }

    public Set<TransactionID> getLockHolders() {
        return lockHolders;
    }
}
