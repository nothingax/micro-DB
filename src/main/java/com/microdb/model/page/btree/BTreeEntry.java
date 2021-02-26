package com.microdb.model.page.btree;

import com.microdb.model.field.Field;

/**
 * B+树内部节点
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeEntry {
    /**
     * 作为索引的键值
     */
    private Field key;

    /**
     * 左孩子PageID
     */
    private BTreePageID leftChildPageID;

    /**
     * 右孩子PageID
     */
    private BTreePageID rightChildPageID;


    /**
     * 记录键元素，主要记录key的序号
     */
    private KeyItem keyItem;


    public BTreeEntry(Field key, BTreePageID leftChildPageID, BTreePageID rightChildPageID) {
        this.key = key;
        this.leftChildPageID = leftChildPageID;
        this.rightChildPageID = rightChildPageID;
    }

    public Field getKey() {
        return key;
    }

    public BTreePageID getLeftChildPageID() {
        return leftChildPageID;
    }

    public BTreePageID getRightChildPageID() {
        return rightChildPageID;
    }

    public void setKey(Field key) {
        this.key = key;
    }

    public void setLeftChildPageID(BTreePageID leftChildPageID) {
        this.leftChildPageID = leftChildPageID;
    }

    public void setRightChildPageID(BTreePageID rightChildPageID) {
        this.rightChildPageID = rightChildPageID;
    }

    public KeyItem getKeyItem() {
        return keyItem;
    }

    public void setKeyItem(KeyItem keyItem) {
        this.keyItem = keyItem;
    }
}
