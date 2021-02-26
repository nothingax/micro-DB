package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.io.IOException;
import java.util.Iterator;

/**
 * BTreeInternalPage 存储索引Key
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeInternalPage implements Page {

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
        return false;
    }

    @Override
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        return 0;
    }

    @Override
    public void insertRow(Row row) {

    }

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }

    /**
     * 返回该页的迭代器
     */
    public BTreeInternalPageIterator getIterator() {
        return new BTreeInternalPageIterator();
    }

    // =====================================迭代器==========================================

    /**
     * 查找B+树内部节点的迭代器
     */
    public class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
        public BTreeInternalPageIterator() {
        }

        public boolean hasNext() {
            // todo
            throw new UnsupportedOperationException("todo");
        }

        public BTreeEntry next() {// todo
            throw new UnsupportedOperationException("todo");
        }
    }
}
