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
     * key编号，从1开始
     */
    private int keyItemNo;

    public KeyItem(BTreePageID pageID, int keyItemNo) {
        this.pageID = pageID;
        this.keyItemNo = keyItemNo;
    }

    public BTreePageID getPageID() {
        return pageID;
    }

    public int getKeyItemNo() {
        return keyItemNo;
    }
}
