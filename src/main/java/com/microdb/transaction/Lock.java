package com.microdb.transaction;

import com.microdb.model.page.PageID;

import java.util.ArrayList;
import java.util.List;

/**
 * 锁
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
     * 持有该锁的事务
     */
    private List<TransactionID> lockHolders;


    public Lock(PageID pageID) {
        this.pageID = pageID;
        lockHolders = new ArrayList<>();
    }

    /**
     * 添加持有锁的事务
     */
    public void addHolder(TransactionID transactionID) {
        lockHolders.add(transactionID);
    }
}
