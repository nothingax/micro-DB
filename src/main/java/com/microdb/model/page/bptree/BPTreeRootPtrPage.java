package com.microdb.model.page.bptree;

import com.microdb.exception.DbException;
import com.microdb.model.page.Page;

import java.io.*;

/**
 * RootPtrPage，单例，一个表文件只有一个RootPtrPage，
 * RootPtrPage存储树的根节点所在的pageNo、page的类型（internal 或leaf）、第一个HeaderPage指针
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeRootPtrPage extends BPTreePage implements Serializable{
    private static final long serialVersionUID = 8954163249356806976L;
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


    public BPTreeRootPtrPage(BPTreePageID pageID, byte[] pageData) throws IOException {
        this.pageID = pageID;
        deserialize(pageData);

        // 保留页原始数据
        saveBeforePage();
    }

    /**
     * 初始化一块rootPtrPage页面大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[rootPtrPageSizeInByte];
    }

    /**
     * 获取ptr，BPTree文件的第0页
     */
    public static BPTreePageID getRootPtrPageID(int tableId) {
        return new BPTreePageID(tableId, 0, BPTreePageType.ROOT_PTR);
    }

    @Override
    public BPTreePageID getPageID() {
        return pageID;
    }

    /**
     * 获取B+Tree的根节点页
     */
    public BPTreePageID getRootNodePageID() {
        if (rootNodePageNo == 0) {
            return null;
        }
        return new BPTreePageID(pageID.getTableId(), rootNodePageNo, rootNodePageType);
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

    @Override
    public int getMaxSlotNum() {
        return 0;
    }

    @Override
    public Page getBeforePage() {
        try {
            return new BPTreeRootPtrPage(pageID, beforePageData);
        } catch (IOException e) {
            throw new DbException("get before page error", e);
        }
    }

    public void setRootPageID(BPTreePageID bpTreePageID) {
        if (bpTreePageID == null) {
            return;
        }
        this.rootNodePageType = bpTreePageID.getPageType();
        this.rootNodePageNo = bpTreePageID.getPageNo();
    }

    public void setFirstHeaderPageID(BPTreePageID firstHeaderPageID) {
        if (firstHeaderPageID == null) {
            firstHeaderPageNo = 0;
        } else {
            if (firstHeaderPageID.getPageType() != BPTreePageType.HEADER) {
                throw new DbException("first header page iD type must be BPTreePageType.HEADER");
            }
            if (firstHeaderPageID.getTableId() != this.pageID.getTableId()) {
                throw new DbException("page id is not match");
            }
            this.firstHeaderPageNo = firstHeaderPageID.getPageNo();
        }
    }

    public BPTreePageID getFirstHeaderPageID() {
        if ((firstHeaderPageNo == 0)) {
            return null;
        }
        return new BPTreePageID(pageID.getTableId(), firstHeaderPageNo, BPTreePageType.HEADER);
    }

    public void setRootNodePageID(BPTreePageID rootNodePageID) {

        if (rootNodePageID == null) {
            rootNodePageNo = 0;
        } else {
            if (rootNodePageID.getPageType() != BPTreePageType.INTERNAL
                    && rootNodePageID.getPageType() != BPTreePageType.LEAF) {
                throw new DbException("root node page iD type must be BPTreePageType.INTERNAL or BPTreePageType.LEAF");
            }
            if (rootNodePageID.getTableId() != this.pageID.getTableId()) {
                throw new DbException("page id is not match");
            }
            this.rootNodePageNo = rootNodePageID.getPageNo();
            this.rootNodePageType = rootNodePageID.getPageType();
        }
    }
}
