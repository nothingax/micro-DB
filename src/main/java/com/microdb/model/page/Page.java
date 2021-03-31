package com.microdb.model.page;

import com.microdb.model.Row;
import com.microdb.transaction.TransactionID;

import java.io.IOException;
import java.util.Iterator;

/**
 * Page 抽象接口
 * 页，读写磁盘文件数据时以page为基本单位
 *
 * @author zhangjw
 * @version 1.0
 */
public interface Page {
    int SIZE_64B = 64;
    int SIZE_4KB = 4096;
    int SIZE_16KB = SIZE_4KB * 4;

    /**
     * 默认每页4KB
     */
    int defaultPageSizeInByte = SIZE_4KB;

    PageID getPageID();

    /**
     * 序列化page数据
     */
    byte[] serialize() throws IOException;

    /**
     * 反序列化pageData
     */
    void deserialize(byte[] pageData) throws IOException;

    // /**
    //  * 计算每页可存放的行数
    //  */
    // int calculateMaxSlotNum(TableDesc tableDesc);

    /**
     * 返回每页可存放的行数
     */
    int getMaxSlotNum();

    boolean isSlotUsed(int index);

    boolean hasEmptySlot();

    Iterator<Row> getRowIterator();

    /**
     * 标记是否为脏页
     */
    void markDirty(TransactionID transactionID);

    boolean isDirty();

    TransactionID getDirtyTid();

    /**
     * 保留页原始数据
     */
    void saveBeforePage();
    /**
     * 获取页在修改前的数据
     */
    Page getBeforePage();
}
