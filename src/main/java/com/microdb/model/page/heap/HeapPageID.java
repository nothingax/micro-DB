package com.microdb.model.page.heap;

import com.microdb.model.page.PageID;

/**
 * pageID用于封装页编号、引用tableId，便于在page中使用tableDesc
 *
 * @author zhangjw
 * @version 1.0
 */
public class HeapPageID implements PageID {
    /**
     * 表Id
     */
    private int tableId;

    /**
     * 页编号
     */
    private int pageNo;

    public HeapPageID(int tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }
}
