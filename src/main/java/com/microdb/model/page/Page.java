package com.microdb.model.page;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;

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
    /**
     * 默认每页4KB
     */
    int defaultPageSizeInByte = 4096;

    PageID getPageID();

    /**
     * 序列化page数据
     */
    byte[] serialize() throws IOException;

    /**
     * 反序列化pageData到page对象
     */
    void deserialize(byte[] pageData) throws IOException;

    boolean hasEmptySlot();

    boolean isSlotUsed(int index);

    /**
     * 计算返回一页数据可存放的数据行数
     */
    int calculateMaxSlotNum(TableDesc tableDesc);

    void insertRow(Row row);

    Iterator<Row> getRowIterator();
}
