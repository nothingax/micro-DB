package com.microdb.transaction;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务ID
 *
 * @author zhangjw
 * @version 1.0
 */
public class TransactionID {
    private static final AtomicLong counter = new AtomicLong(0);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionID that = (TransactionID) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public long serialize() {
        return id;
    }
}
