package com.microdb.model.page.bptree;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.page.Page;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

/**
 * headerPage 用来维护整个file中的pageNo的使用状态，所有headerPage串接成一个双向链表
 * 存储格式：槽位状态标记（每一个slot占用1个字节）、prevHeaderPageNo（4字节）、nextHeaderPageNo（4字节）
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeHeaderPage extends BPTreePage implements Serializable{
    private static final long serialVersionUID = 6252559806123400153L;

    /**
     * prevHeaderPageNo指针 和 nextHeaderPageNo指针的占用空间
     */
    public static final int POINTER_SIZE_IN_BYTE = 4;

    /**
     * slot使用状态标识位图
     * 每个槽位记录文件中的一个PageNo的使用状态，占用空间为1Byte，
     * slotUsageStatusBitMap[i] 表示的是File中 prePageCount * BPTreeHeaderPage.maxSlotNum + i 个pageNo的使用状态
     */
    private boolean[] slotUsageStatusBitMap;

    /**
     * 上一页
     */
    private int prevHeaderPageNo;

    /**
     * 下一页
     */
    private int nextHeaderPageNo;

    /**
     * 槽位数量
     */
    public static final int maxSlotNum = Page.defaultPageSizeInByte - 2 * BPTreeHeaderPage.POINTER_SIZE_IN_BYTE;

    public BPTreeHeaderPage(BPTreePageID bpTreePageID, byte[] pageData) throws IOException {
        this.pageID = bpTreePageID;
        deserialize(pageData);
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Page.defaultPageSizeInByte);
        DataOutputStream dos = new DataOutputStream(baos);
        // 1. slotUsageStatusBitMap
        for (boolean b : slotUsageStatusBitMap) {
            dos.writeBoolean(b);
        }
        // 2.prevHeaderPageNo
        dos.writeInt(prevHeaderPageNo);
        // 3.prevHeaderPageNo
        dos.writeInt(nextHeaderPageNo);
        return baos.toByteArray();
    }

    @Override
    public void deserialize(byte[] pageData) throws IOException {
        DataInputStream bis = new DataInputStream(new ByteArrayInputStream(pageData));
        // 1. slotUsageStatusBitMap
        this.slotUsageStatusBitMap = new boolean[maxSlotNum];
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = bis.readBoolean();
        }
        // 2.prevHeaderPageNo
        this.prevHeaderPageNo = bis.readInt();
        // 3.prevHeaderPageNo
        this.nextHeaderPageNo = bis.readInt();
        bis.close();
    }

    @Override
    public boolean hasEmptySlot() {
        return false;
    }

    @Override
    public boolean isSlotUsed(int index) {
        return false;
    }

    /**
     * 每个字节占用一个槽位
     */
    @Override
    public int getMaxSlotNum() {
        return maxSlotNum;
    }

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }

    public BPTreePageID getNextPageID() {
        if (nextHeaderPageNo == 0) {
            return null;
        }
        return new BPTreePageID(pageID.getTableId(), nextHeaderPageNo, BPTreePageType.HEADER);
    }

    public void setNextHeaderPageNoID(BPTreePageID nextHeaderPageNoID) {
        if (nextHeaderPageNoID == null) {
            nextHeaderPageNo = 0;
        } else {
            if (nextHeaderPageNoID.getTableId() != pageID.getTableId()) {
                throw new DbException("table id not match");
            }
            if (nextHeaderPageNoID.getPageType() != BPTreePageType.HEADER) {
                throw new DbException("page tye must be BPTreePageType.HEADER");
            }
            nextHeaderPageNo = nextHeaderPageNoID.getPageNo();
        }
    }

    public void setPrevHeaderPageID(BPTreePageID prevHeaderPageID) {
        if (prevHeaderPageID == null) {
            prevHeaderPageNo = 0;
        } else {
            if (prevHeaderPageID.getTableId() != pageID.getTableId()) {
                throw new DbException("table id not match");
            }
            if (prevHeaderPageID.getPageType() != BPTreePageType.HEADER) {
                throw new DbException("page tye must be BPTreePageType.HEADER");
            }

            prevHeaderPageNo = prevHeaderPageID.getPageNo();
        }
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

    /**
     * 设置所有slot为使用状态
     */
    public void markAllUsed() {
        Arrays.fill(slotUsageStatusBitMap, true);
    }
}
