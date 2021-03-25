package com.microdb.transaction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务ID
 *
 * @author zhangjw
 * @version 1.0
 */
public class TransactionID {
    private AtomicLong counter = new AtomicLong();

    /**
     * 自增id
     */
    private final long id;

    public TransactionID() {
        id = counter.incrementAndGet();
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
