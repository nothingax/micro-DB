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
    private PageID pageId;

    /**
     * key编号，从1开始，区别与index
     */
    int keyItemNo;

    public KeyItem(PageID pageId, int keyItemNo) {
        this.pageId = pageId;
        this.keyItemNo = keyItemNo;
    }
}
