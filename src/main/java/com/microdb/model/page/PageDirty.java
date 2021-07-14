package com.microdb.model.page;

import com.microdb.transaction.TransactionID;

/**
 * 脏页状态维护抽象类，提供统一的脏页属性和方法
 *
 * @author zhangjw
 * @version 1.0
 */
public abstract class PageDirty implements Page {
    /**
     * 是否是脏页，如果页面被修改过，设置事务ID，通过事务id判断是否是脏页
     */
    protected TransactionID dirtyTxId = null;

    @Override
    public void markDirty(TransactionID transactionID) {
        dirtyTxId = transactionID;
    }

    @Override
    public boolean isDirty() {
        return dirtyTxId != null;
    }

    @Override
    public TransactionID getDirtyTxId() {
        return dirtyTxId;
    }
}
