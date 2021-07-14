package com.microdb.model.page.bptree;

import com.microdb.exception.DbException;
import com.microdb.model.page.PageCommon;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.page.Page;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * b+树各类页的公共属性、方法
 *
 * @author zhangjw
 * @version 1.0
 */
public abstract class BPTreePage extends PageCommon implements Page, Serializable {

    private static final long serialVersionUID = -6147293644376452185L;
    /**
     * 页ID
     */
    protected BPTreePageID pageID;

    /**
     * 父节点pageNo,可能是internal节点或者是rootPtr节点
     */
    protected int parentPageNo;

    /**
     * 索引字段在表结构中的下标
     */
    protected int keyFieldIndex;

    /**
     * 表结构
     */
    protected TableDesc tableDesc;

    @Override
    public BPTreePageID getPageID() {
        return pageID;
    }

    public abstract byte[] serialize() throws IOException;

    public abstract void deserialize(byte[] pageData) throws IOException;

    public abstract boolean hasEmptySlot();

    public abstract boolean isSlotUsed(int index);

    // public abstract int calculateMaxSlotNum(TableDesc tableDesc);

    public abstract Iterator<Row> getRowIterator();

    @Override
    public void saveBeforePage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getBeforePage() {
        throw new UnsupportedOperationException();
    }


    /**
     * 向dos中填充指定数量的字节
     *
     * @param dos      DataOutputStream
     * @param bytesNum 填充的字节数量
     * @throws IOException write byte error
     */
    protected void fillBytes(DataOutputStream dos, int bytesNum) throws IOException {
        if (dos == null) {
            throw new DbException("fill bytes error: stream is closed ");
        }
        byte[] emptyBytes = new byte[bytesNum];
        dos.write(emptyBytes, 0, bytesNum);
    }

    public void setParentPageID(BPTreePageID parentPageID) {
        if (parentPageID == null) {
            throw new DbException("parent id must not be null");
        }
        if (parentPageID.getTableId() != pageID.getTableId()) {
            throw new DbException("table id mismatch");
        }

        if (parentPageID.getPageType() == BPTreePageType.ROOT_PTR) {
            parentPageNo = 0;
        } else if (parentPageID.getPageType() == BPTreePageType.INTERNAL) {
            parentPageNo = parentPageID.getPageNo();
        } else {
            throw new DbException("parent must be internal or root node");
        }
    }

    public BPTreePageID getParentPageID() {
        if (parentPageNo == 0) { // 没有内部节点时，父节点是RootPtr
            return new BPTreePageID(this.pageID.getTableId(), 0, BPTreePageType.ROOT_PTR);
        }
        return new BPTreePageID(this.pageID.getTableId(), parentPageNo, BPTreePageType.INTERNAL);
    }

}
