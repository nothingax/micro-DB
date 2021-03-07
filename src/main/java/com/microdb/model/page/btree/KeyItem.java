package com.microdb.model.page.btree;

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
    private BTreePageID pageID;

    /**
     * 元素在页中的槽位下标
     */
    private int slotIndex;

    public KeyItem(BTreePageID pageID, int slotIndex) {
        this.pageID = pageID;
        this.slotIndex = slotIndex;
    }

    public BTreePageID getPageID() {
        return pageID;
    }

    public int getSlotIndex() {
        return slotIndex;
    }
}
