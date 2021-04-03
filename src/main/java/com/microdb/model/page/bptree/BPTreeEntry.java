package com.microdb.model.page.bptree;

import com.microdb.model.field.Field;

/**
 * B+树内部节点
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeEntry {
    /**
     * 作为索引的键值
     */
    private Field key;

    /**
     * 左孩子PageID
     */
    private BPTreePageID leftChildPageID;

    /**
     * 右孩子PageID
     */
    private BPTreePageID rightChildPageID;


    /**
     * 记录键元素，主要记录key的序号
     */
    private RowID rowID;


    public BPTreeEntry(Field key, BPTreePageID leftChildPageID, BPTreePageID rightChildPageID) {
        this.key = key;
        this.leftChildPageID = leftChildPageID;
        this.rightChildPageID = rightChildPageID;
    }

    public Field getKey() {
        return key;
    }

    public BPTreePageID getLeftChildPageID() {
        return leftChildPageID;
    }

    public BPTreePageID getRightChildPageID() {
        return rightChildPageID;
    }

    public void setKey(Field key) {
        this.key = key;
    }

    public void setLeftChildPageID(BPTreePageID leftChildPageID) {
        this.leftChildPageID = leftChildPageID;
    }

    public void setRightChildPageID(BPTreePageID rightChildPageID) {
        this.rightChildPageID = rightChildPageID;
    }

    public RowID getRoeID() {
        return rowID;
    }

    public void setRowID(RowID rowID) {
        this.rowID = rowID;
    }

    public int getLeftChildPageType() {
        return this.getLeftChildPageID().getPageType();
    }

    public int getRightChildPageType() {
        return this.getRightChildPageID().getPageType();
    }

    @Override
    public String toString() {
        return "BPTreeEntry{" +
                "key=" + key +
                ", leftChildPage=" + leftChildPageID.getPageNo() +
                ", rightChildPage=" + rightChildPageID.getPageNo() +
                ", rowIDSlotIndex=" + rowID.getSlotIndex() +
                '}';
    }
}
