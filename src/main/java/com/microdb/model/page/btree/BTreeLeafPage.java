package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.io.IOException;
import java.util.Iterator;

/**
 * TODO
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeLeafPage implements Page {

    /**
     * 初始化一块leafPae页面默认大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[Page.defaultPageSizeInByte];
    }

    @Override
    public PageID getPageID() {
        return null;
    }

    @Override
    public byte[] serialize() throws IOException {
        return new byte[0];
    }

    @Override
    public boolean hasEmptySlot() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        return 0;
    }

    @Override
    public void insertRow(Row row) {

        // 更新rootPtr
    }

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }
}
