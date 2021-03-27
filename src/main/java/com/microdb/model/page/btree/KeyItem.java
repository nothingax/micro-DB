package com.microdb.model.page.btree;

import com.microdb.model.page.PageID;

/**
 * 内部页中键元素
 * 封装键的编号和所在page
 *
 * @author zhangjw
 * @version 1.0
 */
public class KeyItem {
    /**
     * pageID
     */
    private PageID pageID;

    /**
     * 元素在页中的槽位下标
     */
    private int slotIndex;

    public KeyItem(PageID pageID, int slotIndex) {
        this.pageID = pageID;
        this.slotIndex = slotIndex;
    }

    public PageID getPageID() {
        return pageID;
    }

    public int getSlotIndex() {
        return slotIndex;
    }
}
