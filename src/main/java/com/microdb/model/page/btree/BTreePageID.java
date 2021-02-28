package com.microdb.model.page.btree;

import com.microdb.model.page.PageID;

/**
 * B+Tree pageID
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreePageID implements PageID {

    /**
     * 存储B+树根节点指针的页
     */
    public final static int TYPE_ROOT_PTR = 0;
    /**
     * 存储页面的使用状态，链表扩展
     */
    public final static int TYPE_HEADER = 1;
    /**
     * 存储B+树的内部节点
     */
    public final static int TYPE_INTERNAL = 2;
    /**
     * 存储B+树的页子节点，在聚簇索引树中，页子节点存储的是整行数据
     */
    public final static int TYPE_LEAF = 3;

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