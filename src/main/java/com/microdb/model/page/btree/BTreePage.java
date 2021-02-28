package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;

import java.io.IOException;
import java.util.Iterator;

/**
 * b+树各类页的公共属性、方法
 *
 * @author zhangjw
 * @version 1.0
 */
public abstract class BTreePage implements Page {


    /**
     * 该页ID
     */
    protected BTreePageID pageID;

    /**
     * 父节点pageNo,可能是internal节点或者是rootPtr节点
     */
    protected int parentPageNo;

    public void setParentPageID(BTreePageID parentPageID) {
        if (parentPageID == null) {
            throw new DbException("parent id must not be null");
        }
        if (parentPageID.getTableId() != pageID.getTableId()) {
            throw new DbException("table id mismatch");
        }

        if (parentPageID.getPageType() != BTreePageID.TYPE_ROOT_PTR) {
            parentPageNo = 0;
        } else if (parentPageID.getPageType() != BTreePageID.TYPE_INTERNAL) {
            parentPageNo = parentPageID.getPageNo();
        } else {
            throw new DbException("parent must be internal or root node");
        }
    }

    public BTreePageID getParentPageID() {
        if (parentPageNo == 0) { // 没有内部节点时，父节点是RootPtr
            new BTreePageID(this.pageID.getTableId(), 0, BTreePageID.TYPE_ROOT_PTR);
        }
        return new BTreePageID(this.pageID.getTableId(), parentPageNo, BTreePageID.TYPE_INTERNAL);
    }

    @Override
    public BTreePageID getPageID() {
        return pageID;
    }

    public abstract byte[] serialize() throws IOException;

    public abstract void deserialize(byte[] pageData) throws IOException;

    @Override
    public boolean hasEmptySlot() {
        return false;
    }

    @Override
    public boolean isSlotUsed(int index) {
        return false;
    }

    @Override
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        return 0;
    }

    public abstract void insertRow(Row row);

    public abstract Iterator<Row> getRowIterator();
}
