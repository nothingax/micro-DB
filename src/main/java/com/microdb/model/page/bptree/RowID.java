package com.microdb.model.page.bptree;

import com.microdb.model.page.PageID;

import java.io.Serializable;

/**
 * 内部页中键元素
 * 封装键的编号和所在page
 *
 * @author zhangjw
 * @version 1.0
 */
public class RowID implements Serializable {
    private static final long serialVersionUID = -6571929580121756569L;

    /**
     * pageID
     */
    private final PageID pageID;

    /**
     * 元素在页中的槽位下标
     */
    private final int slotIndex;

    public RowID(PageID pageID, int slotIndex) {
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
