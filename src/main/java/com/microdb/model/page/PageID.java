package com.microdb.model.page;

/**
 * pageID用于封装页编号、引用tableId，便于在page中使用tableDesc
 *
 * @author zhangjw
 * @version 1.0
 */
public class PageID {
    /**
     * 表Id
     */
    private int tableId;

    /**
     * 页编号
     */
    private int pageNo;

    public PageID(int tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    public int getTableId() {
        return tableId;
    }

    public int getPageNo() {
        return pageNo;
    }
}