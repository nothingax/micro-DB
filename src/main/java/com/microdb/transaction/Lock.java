package com.microdb.transaction;

import com.microdb.model.page.PageID;

/**
 * 锁
 * 初步设计：支持独占锁(x lock)
 * <p>
 * TODO 实现共享锁(s lock)
 *
 * @author zhangjw
 * @version 1.0
 */
public class Lock {

    /**
     * 被锁的页
     */
    private PageID pageID;

    /**
     * 持有该锁的事务,独占锁：一个页只能被一个事务持有
     * 后续实现共享锁(s lock)
     */
    private TransactionID lockHolder;

    // /**
    //  * 持有该锁的事务
    //  */
    // private List<TransactionID> lockHolders;


    public Lock(PageID pageID) {
        this.pageID = pageID;
        // lockHolders = new ArrayList<>();
    }

    public void setLockHolder(TransactionID lockHolder) {
        this.lockHolder = lockHolder;
    }

    public TransactionID getLockHolder() {
        return lockHolder;
    }

    // /**
    //  * 添加持有锁的事务
    //  */
    // public void addHolder(TransactionID transactionID) {
    //     lockHolders.add(transactionID);
    // }
}
