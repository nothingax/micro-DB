package com.microdb.config;

/**
 * 数据库配置
 *
 * @author zhangjw
 * @version 1.0
 */
public class DBConfig {

    /**
     * 页的大小
     */
    private final int pageSizeInByte;

    /**
     * 缓存池容量，单位：页
     */
    private final int bufferPoolCapacity;

    public DBConfig(int pageSizeInByte, int bufferPoolCapacity) {
        this.pageSizeInByte = pageSizeInByte;
        this.bufferPoolCapacity = bufferPoolCapacity;
    }

    public int getPageSizeInByte() {
        return pageSizeInByte;
    }

    public int getBufferPoolCapacity() {
        return bufferPoolCapacity;
    }
}
