package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;

import java.io.*;
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
    public static final int rootPtrPageSizeInByte = 9;

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


    public BTreeRootPtrPage(BTreePageID pageID, byte[] pageData) throws IOException {
        this.pageID = pageID;
        deserialize(pageData);
    }

    /**
     * 初始化一块rootPtrPage页面大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[rootPtrPageSizeInByte];
    }

    public static BTreePageID getRootNodePageID(int tableId) {
        return new BTreePageID(tableId, 0, BTreePageType.ROOT_PTR);
    }

    @Override
    public BTreePageID getPageID() {
        return pageID;
    }

    public BTreePageID getRootNodePageID() {
        if (rootNodePageNo == 0) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(), rootNodePageNo, rootNodePageType);
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

    /**
     * 反序列化，按序读取9个字节
     */
    @Override
    public void deserialize(byte[] pageData) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(pageData));
        // 4
        this.rootNodePageNo = dataInputStream.readInt();
        // 1
        this.rootNodePageType = dataInputStream.readByte();
        // 4
        this.firstHeaderPageNo = dataInputStream.readInt();
    }

    @Override
    public boolean hasEmptySlot() {
        return false;
    }

    @Override
    public boolean isSlotUsed(int index) {
        return false;
    }

    public int calculateMaxSlotNum(TableDesc tableDesc) {
        return 0;
    }

    @Override
    public int getMaxSlotNum() {
        return 0;
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

    public void setFirstHeaderPageID(BTreePageID firstHeaderPageID) {
        if (firstHeaderPageID == null) {
            firstHeaderPageNo = 0;
        } else {
            if (firstHeaderPageID.getPageType() != BTreePageType.HEADER) {
                throw new DbException("first header page iD type must be BTreePageType.HEADER");
            }
            if (firstHeaderPageID.getTableId() != this.pageID.getTableId()) {
                throw new DbException("page id is not match");
            }
            this.firstHeaderPageNo = firstHeaderPageID.getPageNo();
        }
    }

    public BTreePageID getFirstHeaderPageID() {
        if ((firstHeaderPageNo == 0)) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(), firstHeaderPageNo, BTreePageType.HEADER);
    }

    public void setRootNodePageID(BTreePageID rootNodePageID) {

        if (rootNodePageID == null) {
            rootNodePageNo = 0;
        } else {
            if (rootNodePageID.getPageType() != BTreePageType.INTERNAL
                    || rootNodePageID.getPageType() != BTreePageType.LEAF) {
                throw new DbException("root node page iD type must be BTreePageType.INTERNAL or BTreePageType.LEAF");
            }
            if (rootNodePageID.getTableId() != this.pageID.getTableId()) {
                throw new DbException("page id is not match");
            }
            this.rootNodePageNo = rootNodePageID.getPageNo();
        }
    }
}
