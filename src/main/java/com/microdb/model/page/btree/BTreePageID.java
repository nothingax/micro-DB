package com.microdb.model.page.btree;

import com.microdb.model.page.PageID;

/**
 * B+Tree pageID
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreePageID implements PageID {

    public final static int ROOT_PTR = 0;
    public final static int INTERNAL = 1;
    public final static int LEAF = 2;
    public final static int HEADER = 3;


    /**
     * 表ID
     */
    private int tableId;

    /**
     * page 编号
     */
    private int pageNo;

    /**
     * 页类型
     */
    private int pageType;

    public BTreePageID(int tableId, int pageNo, int pageType) {
        this.tableId = tableId;
        this.pageNo = pageNo;
        this.pageType = pageType;
    }

    @Override
    public int getTableId() {
        return 0;
    }

    @Override
    public int getPageNo() {
        return 0;
    }

    public int getPageType() {
        return pageType;
    }
}
