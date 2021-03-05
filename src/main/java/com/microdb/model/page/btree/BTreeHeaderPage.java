package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;

import java.io.IOException;
import java.util.Iterator;

/**
 * headerPage 用来维护整个file中的pageNo的使用状态，所有headerPage串接成一个双向链表
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeHeaderPage extends BTreePage {
    /**
     * prevHeaderPageNo 和 nextHeaderPageNo的空间
     */
    public static final int POINTER_SIZE_IN_BYTE = 4;


    /**
     * 上一页
     */
    private int prevHeaderPageNo;

    /**
     * 下一页
     */
    private int nextHeaderPageNo;

    /**
     * slot使用状态标识位图
     * 每个槽位记录文件中的一个PageNo的使用状态，占用空间为1Byte，
     * slotUsageStatusBitMap[i] 表示的是File中 prePageCount * BTreeHeaderPage.maxSlotNum + i 个pageNo的使用状态
     */
    private boolean[] slotUsageStatusBitMap;


    /**
     * headerPage中的槽位数量
     */
    public static final int maxSlotNum = Page.defaultPageSizeInByte - 2 * BTreeHeaderPage.POINTER_SIZE_IN_BYTE;


    public BTreeHeaderPage(BTreePageID bTreePageID, byte[] pageData) {

    }

    @Override
    public byte[] serialize() throws IOException {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] pageData) throws IOException {

    }

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

    /**
     * 每个字节占用一个槽位
     */
    @Override
    public int getMaxSlotNum() {
        return 0;
    }

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }

    public BTreePageID getNextPageID() {
        if (nextHeaderPageNo == 0) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(), nextHeaderPageNo, BTreePageType.HEADER);
    }

    /**
     * 返回第一个空slot的序号
     */
    public int getFirstEmptySlot() {
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            if (!slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }

    public void markSlotUsed(int slot, boolean status) {
        slotUsageStatusBitMap[slot] = status;
    }
}
