package com.microdb.model.page;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;

import java.io.IOException;
import java.util.Iterator;

/**
 * Page 抽象接口
 *
 * @author zhangjw
 * @version 1.0
 */
public interface Page {
    int getPageNo();

    /**
     * 序列化page数据
     */
    byte[] serialize() throws IOException;

    boolean hasEmptySlot();

    /**
     * 计算返回一页数据可存放的数据行数
     */
    int calculateMaxSlotNum(TableDesc tableDesc);

    void insertRow(Row row);

    Iterator<Row> getRowIterator();
}
