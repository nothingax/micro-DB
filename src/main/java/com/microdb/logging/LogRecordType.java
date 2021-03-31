package com.microdb.logging;

/**
 * log 记录类型
 *
 * @author zhangjw
 * @version 1.0
 */
public interface LogRecordType {
    int TX_START = 0;
    int PAGE_FLUSH = 1;
    int TX_COMMIT = 2;
}
