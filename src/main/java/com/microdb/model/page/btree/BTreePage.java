package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;

import java.io.IOException;
import java.util.Iterator;

/**
 * TODO
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreePage implements Page {
    @Override
    public int getPageNo() {
        return 0;
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
}
