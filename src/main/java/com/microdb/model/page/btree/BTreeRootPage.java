package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;

import java.io.IOException;
import java.util.Iterator;

/**
 * rootPage，单例，一个表文件只存在一个rootPage，
 * rootPage存储树的根节点所在的pageNo、page的类型（internal 或leaf）、第一个HeaderPage指针
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeRootPage implements Page {

    /**
     * 该BTreeRootPage的pageID
     */
    private BTreePageID pageID;

    /**
     * 根节点所在的pageNo
     */
    private int rootNodePageNo;

    /**
     * 根节点所在页的类型
     */
    private int rootNodePageType;

    /**
     * TODO
     */
    public static byte[] createEmptyPageData() {
        return new byte[0];
    }

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


    public int getRootNodePageNo() {
        return rootNodePageNo;
    }

    public int getRootNodePageType() {
        return rootNodePageType;
    }

    public void setRootPageID(BTreePageID bTreePageID) {
        if (bTreePageID == null) {
            return;
        }
        this.rootNodePageType = bTreePageID.getPageType();
        this.rootNodePageNo = bTreePageID.getPageNo();
    }
}
