package com.microdb.model.page;

import com.microdb.transaction.TransactionID;

/**
 * 脏页状态维护抽象类，提供统一的脏页属性和方法
 *
 * @author zhangjw
 * @version 1.0
 */
public abstract class DirtyPage implements Page {
    /**
     * 是否是脏页，如果页面被修改过，设置事务ID，通过事务id判断是否是脏页
     */
    protected TransactionID dirtyTid = null;

    @Override
    public void markDirty(TransactionID transactionID) {
        dirtyTid = transactionID;
    }

    @Override
    public boolean isDirty() {
        return dirtyTid != null;
    }

    @Override
    public TransactionID getDirtyTid() {
        return dirtyTid;
    }
}
