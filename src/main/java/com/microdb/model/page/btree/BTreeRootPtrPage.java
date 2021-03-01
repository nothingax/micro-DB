package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * RootPtrPage，单例，一个表文件只有一个RootPtrPage，
 * RootPtrPage存储树的根节点所在的pageNo、page的类型（internal 或leaf）、第一个HeaderPage指针
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeRootPtrPage extends BTreePage {

    /**
     * 9字节 4+1+4
     * 4:rootNode的pageNo
     * 1:rootNode的page的类型
     * 4:第一个HeaderPage的pageNo
     */
    public static int rootPtrPageSizeInByte = 9;

    /**
     * 根节点所在的pageNo
     */
    private int rootNodePageNo;

    /**
     * 根节点所在页的类型
     */
    private int rootNodePageType;

    /**
     * 第一个headerPage的PageNo
     */
    private int firstHeaderPageNo;

    /**
     * 初始化一块rootPtrPage页面大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[rootPtrPageSizeInByte];
    }

    @Override
    public BTreePageID getPageID() {
        return pageID;
    }

    public BTreePageID getRootNodePageID() {
        if (rootNodePageNo == 0) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(),rootNodePageNo,rootNodePageType);
    }


    /**
     * 序列化
     */
    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(rootPtrPageSizeInByte);
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(rootNodePageNo);
        dos.writeByte(rootNodePageType);
        dos.writeInt(firstHeaderPageNo);
        dos.flush();

        return baos.toByteArray();
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

    @Override
    public int getMaxSlotNum() {
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

    public BTreePageID getFirstHeaderPageID() {
        if ((firstHeaderPageNo == 0)) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(), firstHeaderPageNo, BTreePageType.HEADER);
    }

    public void setRootNodePageID(BTreePageID pageID) {
    }
}
