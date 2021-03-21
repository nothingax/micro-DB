package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.Page;
import com.microdb.operator.PredicateEnum;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * BTreeInternalPage 存储索引Key
 * 格式：槽位状态标记（每一个slot占用1Byte）、父指针(4Byte)、子页类型（1Byte）、
 * 索引键值（m+1个位置，位置0不存储）、子节点指针(m+1 Byte)
 * m个key
 * m个slot mByte
 * m个ChildPages ：2*m*4B
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeInternalPage extends BTreePage {

    // TODO 重写
    public class ChildPages {
        int leftPageNo;
        int rightPageNo;

        public ChildPages() {
        }

        public ChildPages(int leftPageNo, int rightPageNo) {
            this.leftPageNo = leftPageNo;
            this.rightPageNo = rightPageNo;
        }

        public ChildPages(BTreeEntry entry) {
            this.leftPageNo = entry.getLeftChildPageID().getPageNo();
            this.rightPageNo = entry.getRightChildPageID().getPageNo();
        }
    }

    /**
     * 指针长度，4个字节
     */
    private static final int INDEX_SIZE_IN_BYTE = FieldType.INT.getSizeInByte();

    /**
     * slot使用状态标识位图
     * 为利用 {@link DataOutputStream#writeBoolean(boolean)} api的便利性，
     * 物理文件使用一个byte存储一个状态位
     */
    private boolean[] slotUsageStatusBitMap;

    /**
     * 子节点page类型
     */
    private int childrenPageType;

    /**
     * 所有子节点的page编号
     * 本页中有 m 个key，则对应的子节点有m+1个
     * size size为key的数量+1
     */
    // private int[] childrenPageNos;

    // TODO 重写
    private ChildPages[] childPages;

    /**
     * 存储本页中的key
     * keyList size为key的数量+1, index 0 不存储内容
     */
    private Field[] keys;

    /**
     * 一页数据最多可存放的节点元素数量
     */
    private int maxSlotNum;

    public BTreeInternalPage(BTreePageID bTreePageID, byte[] pageData, int keyFieldIndex) throws IOException {
        this.pageID = bTreePageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(bTreePageID.getTableId()).getTableDesc();
        this.maxSlotNum = calculateMaxSlotNum(tableDesc);
        this.keyFieldIndex = keyFieldIndex;
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

        // 2. parentPageNo
        dos.writeInt(parentPageNo);
        // 3. childrenPageType
        dos.writeByte(childrenPageType);

        // 4. keys，从1开始，0位置不存储索引
        int sizePerKeyInByte = tableDesc.getFieldTypes().get(keyFieldIndex).getSizeInByte();
        for (int i = 0; i < keys.length; i++) {
            if (isSlotUsed(i)) {
                keys[i].serialize(dos);
            } else {
                // 填充 keySizeInByte 个字节
                fillBytes(dos, sizePerKeyInByte);
            }
        }

        // 5. childrenPageNos
        for (int i = 0; i < childPages.length; i++) {
            if (isSlotUsed(i)) {
                ChildPages childPage = childPages[i];
                dos.writeInt(childPage.leftPageNo);
                dos.writeInt(childPage.rightPageNo);
            } else {
                // 填充 INDEX_SIZE_IN_BYTE 个字节
                fillBytes(dos, INDEX_SIZE_IN_BYTE * 2);
            }
        }

        // 6、填充剩余空间
        int slotSize = slotUsageStatusBitMap.length;
        int parentPageNoSize = INDEX_SIZE_IN_BYTE;
        int childrenPageTypeSize = 1;
        int keysSize = sizePerKeyInByte * (keys.length);
        int childrenNosSize = childPages.length * INDEX_SIZE_IN_BYTE * 2;// childPages 左右两个页面
        int paddingLength = Page.defaultPageSizeInByte
                - slotSize - parentPageNoSize - childrenPageTypeSize - keysSize - childrenNosSize;
        fillBytes(dos, paddingLength);

        dos.flush();
        return baos.toByteArray();
    }

    @Override
    public void deserialize(byte[] pageData) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(pageData));
        // 1. slotUsageStatusBitMap
        slotUsageStatusBitMap = new boolean[maxSlotNum];
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = dis.readBoolean();
        }

        // 2. parentPageNo
        parentPageNo = dis.readInt();
        // 3. childrenPageType
        childrenPageType = dis.readByte();

        // 4. keys
        keys = new Field[maxSlotNum];
        // 0位置不存放元素
        // keys[0] = null;
        FieldType keyFieldType = tableDesc.getFieldTypes().get(keyFieldIndex);
        for (int i = 0; i < keys.length; i++) {
            if (isSlotUsed(i)) {
                keys[i] = keyFieldType.parse(dis);
            } else {
                keys[i] = null;
                for (int i1 = 0; i1 < keyFieldType.getSizeInByte(); i1++) {
                    dis.readByte();
                }
            }
        }

        // 5. childrenPageNos
        childPages = new ChildPages[maxSlotNum];
        for (int i = 0; i < childPages.length; i++) {
            if (isSlotUsed(i)) {
                ChildPages child = new ChildPages();
                child.leftPageNo = ((IntField) FieldType.INT.parse(dis)).getValue();
                child.rightPageNo = ((IntField) FieldType.INT.parse(dis)).getValue();
                this.childPages[i] = child;
            } else {
                for (int i1 = 0; i1 < INDEX_SIZE_IN_BYTE * 2; i1++) {
                    dis.readByte();
                }
            }
        }

        dis.close();
    }

    @Override
    public boolean hasEmptySlot() {
        for (boolean b : this.slotUsageStatusBitMap) {
            if (!b) {
                return true;
            }
        }
        return false;
    }

    /**
     * 页中第index个槽位是否存在内容
     */
    @Override
    public boolean isSlotUsed(int index) {
        return slotUsageStatusBitMap[index];
    }

    public int calculateMaxSlotNum(TableDesc tableDesc) {
        return getMaxEntryNum(tableDesc);
    }

    private int getMaxEntryNum(TableDesc tableDesc) {
        int slotStatusSizeInByte = 1;
        int pageNoSizeInByte = FieldType.INT.getSizeInByte();
        int keySizeInByte = tableDesc.getFieldTypes().get(keyFieldIndex).getSizeInByte();

        // 索引值+子节点指针（每个entry两个子页面）+slot状态位
        int perEntrySizeInByte = keySizeInByte + pageNoSizeInByte * 2 + slotStatusSizeInByte;

        // // 每页一个父指针，m个entry有m+1个子节点指针，一个字节表示子page类型、一个额外的header使用1字节。
        // int extra = 2 * FieldType.INT.getSizeInByte() + 1 + 1;

        // 每页一个父pageNo指针 一个字节表示子page类型
        int extra = FieldType.INT.getSizeInByte() + 1;

        return (Page.defaultPageSizeInByte - extra) / perEntrySizeInByte;
    }

    @Override
    public int getMaxSlotNum() {
        return maxSlotNum;
    }

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }

    /**
     * 已存在的数量
     */
    public int getExistCount() {
        int cnt = 0;

        // for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
        //     cnt += slotUsageStatusBitMap[i] ? 1 : 0;
        // }
        for (boolean b : slotUsageStatusBitMap) {
            cnt += b ? 1 : 0;
        }

        return cnt;
    }

    /**
     * 获取第index个子节点pageID
     * 通过页内存储的子节点的pageNo，构造并返回pageID
     */
    private BTreePageID getChildPageIDFromLeft(int index) {
        if (index < 0 || index >= childPages.length) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        int leftPageNo = childPages[index].leftPageNo;
        return new BTreePageID(pageID.getTableId(), leftPageNo, childrenPageType);
    }

    /**
     * 获取第index个子节点pageID
     * 通过页内存储的子节点的pageNo，构造并返回pageID
     */
    private BTreePageID getChildPageIDFromRight(int index) {
        if (index < 0 || index >= childPages.length) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        int rightPageNo = childPages[index].rightPageNo;
        return new BTreePageID(pageID.getTableId(), rightPageNo, childrenPageType);
    }

    /**
     * 获取第index个节点的键值
     */
    protected Field getKey(int index) {
        if (index < 0 || index >= keys.length) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        return keys[index];
    }

    /**
     * 返回该页的迭代器
     */
    public Iterator<BTreeEntry> getIterator() {
        return new BTreeInternalPageIterator(this);
    }

    /**
     * 找位置，移动数据空出位置，插入节点
     */
    public void insertEntry(BTreeEntry entry) {
        if (childrenPageType == BTreePageType.ROOT_PTR) {
            childrenPageType = entry.getLeftChildPageType();
        }

        // // 第一个节点特殊处理
        // if (getExistCount() == 0) {
        //     childrenPageNos[0] = entry.getLeftChildPageID().getPageNo();
        //     childrenPageNos[1] = entry.getRightChildPageID().getPageNo();
        //
        //
        //     keys[0] = entry.getKey(); // keyList 0位置不存放数据
        //     markSlotUsed(0, true);
        //     // markSlotUsed(1, true);
        //     entry.setKeyItem(new KeyItem(pageID, 0));
        //     return;
        // }

        // 找到第一个空位置，从1开始，因为0不存放数据
        int firstEmptySlot = getFirstEmptySlot();
        if (firstEmptySlot == -1) {
            throw new DbException("insert entry error :no empty slot");
        }


        // 查找本页中与待插入节点有相同左孩子或右孩子的节点。
        int insertIndex = -1;
        for (int i = 0; i < getMaxSlotNum(); i++) {
            if (isSlotUsed(i)) {
                // 找到应插入的位置
                if (keys[i].compare(PredicateEnum.GREATER_THAN_OR_EQ, entry.getKey())) {
                    insertIndex = i + 1;
                    // check
                    ChildPages childPage = childPages[i];
                    int pageNo = entry.getRightChildPageID().getPageNo();
                    if (childPage.rightPageNo == pageNo) {
                        // entry 插在childPage的右侧，childPage[i+1]右移空出位置
                        // insertIndex = i; // changeI
                    } else if (childPage.leftPageNo == pageNo) {
                        // entry 插在childPage的左侧，childPage[i]右移空出位置
                        // rightInsertIndex = i; // changeI+1
                    } else {
                        throw new DbException("子页不相邻");
                    }
                    break;
                }
            }
        }

        int matchedSlot = -1;
        // 左插
        if (insertIndex != -1) {
            if (firstEmptySlot < insertIndex) {
                // 左边有空位，将(firstEmptySlot,leftInsertIndex]向左平移1位
                for (int i = firstEmptySlot + 1; i <= insertIndex - 1; i++) {
                    shift(i, i - 1);
                }
            } else { // 右边有空位[leftInsertIndex+1，firstEmptySlot) 向右平移1
                for (int i = firstEmptySlot - 1; i >= insertIndex; i--) {
                    shift(i, i + 1);
                }
            }

            matchedSlot = insertIndex;
            markSlotUsed(matchedSlot, true);
            keys[matchedSlot] = entry.getKey();

            int before = findEntryBefore(matchedSlot);
            int after = findEntryAfter(matchedSlot);
            if (before != -1 && childPages[before].rightPageNo == entry.getRightChildPageID().getPageNo()) {
                childPages[before].rightPageNo = entry.getLeftChildPageID().getPageNo();
            } else if (before != -1 && childPages[before].rightPageNo == entry.getLeftChildPageID().getPageNo()) {
                if (after != -1) {
                    childPages[after].leftPageNo = entry.getRightChildPageID().getPageNo();
                } else {
                    // matchedSlot should be the last in the page
                }
            } else {
                throw new DbException("error");
            }
            entry.setKeyItem(new KeyItem(pageID, matchedSlot));
        }
    }

    private int findEntryBefore(int matchedSlot) {
        for (int i = matchedSlot; i >= 0; i--) {
            if (slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }

    private int findEntryAfter(int matchedSlot) {
        for (int i = matchedSlot; i < slotUsageStatusBitMap.length; i++) {
            if (slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }

    private void shift(int from, int to) {
        if (!isSlotUsed(to) && isSlotUsed(from)) {
            markSlotUsed(to, true);
            keys[to] = keys[from];
            childPages[to] = childPages[from];
            markSlotUsed(from, false);
        }
    }

    private int getFirstEmptySlot() {
        int emptySlot = -1;
        for (int i = 0; i < this.maxSlotNum; i++) {
            if (!isSlotUsed(i)) {
                emptySlot = i;
                break;
            }
        }
        return emptySlot;
    }

    public Iterator<BTreeEntry> getReverseIterator() {
        return new BTreeInternalPageReverseIterator(this);
    }

    /**
     * 从左边开始删除entry
     */
    public void deleteEntryFromTheLeft(BTreeEntry entry) {
        KeyItem keyItem = entry.getKeyItem();
        markSlotUsed(keyItem.getSlotIndex(), false);
        entry.setKeyItem(null);
        keys[keyItem.getSlotIndex()] = null;
        KeyItem keyItem1 = entry.getKeyItem();
        for (int i = keyItem1.getSlotIndex() - 1; i >= 0; i--) {
            if (isSlotUsed(i)) {
                childPages[i].rightPageNo = childPages[keyItem1.getSlotIndex()].leftPageNo;
                break;
            }
        }
    }

    public void deleteEntryFromTheRight(BTreeEntry entry) {
        KeyItem keyItem = entry.getKeyItem();
        markSlotUsed(keyItem.getSlotIndex(), false);
        entry.setKeyItem(null);
        keys[keyItem.getSlotIndex()] = null;
    }

    public boolean isLessThanHalfFullOpen() {
        return this.getExistCount() < this.getMaxSlotNum() / 2;
    }


    public boolean isLessThanHalfFull() {
        // 4/2=2
        // 5/2=2 2<=2
        // TODO

        // int maxEnty = this.getMaxSlotNum() - 1;
        //
        // int maxEmptySlots = maxEnty - maxEnty / 2;
        // int numEmpy = maxEnty - this.getExistCount();

        // return numEmpy >= maxEmptySlots;
        // return - this.getExistCount() >= -maxEnty / 2;
        return this.getExistCount() <= this.getMaxSlotNum() / 2;
    }

    private void markSlotUsed(int keyItemNo, boolean isUsed) {
        slotUsageStatusBitMap[keyItemNo] = isUsed;
    }

    public boolean isEmpty() {
        return this.getExistCount() == 0;
    }

    /**
     * 更新entry
     */
    public void updateEntry(BTreeEntry entry) {
        KeyItem keyItem = entry.getKeyItem();

        // 校验
        if (keyItem == null) {
            throw new DbException("tried to update entry with null keyItem");
        }
        if (keyItem.getPageID().getTableId() != pageID.getTableId()) {
            throw new DbException("table id not match");
        }

        if (keyItem.getPageID().getPageNo() != pageID.getPageNo()) {
            throw new DbException("page no not match");
        }
        if (!isSlotUsed(keyItem.getSlotIndex())) {
            throw new DbException("update entry error:slot is not used");
        }

        // todo 检验元素顺序

        int slotIndex = keyItem.getSlotIndex();
        childPages[slotIndex] = new ChildPages(entry);
        keys[slotIndex] = entry.getKey();
    }
    // =====================================迭代器==========================================

    /**
     * 查找B+树内部节点的迭代器
     */
    public class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
        /**
         * 当前遍历的key和children的下标
         * TODO
         */
        int curIndex = 0;

        /**
         * 暂存上一次取得的childPageID，在构造BtreeEntry时做临时变量使用
         */
        BTreePageID prevChildPageID = null;

        /**
         * 做临时变量使用，在next()中返回
         */
        BTreeEntry nextEntryToReturn = null;

        BTreeInternalPage internalPage;

        public BTreeInternalPageIterator(BTreeInternalPage internalPage) {
            this.internalPage = internalPage;
        }

        public boolean hasNext() {
            if (nextEntryToReturn != null) {
                return true;
            }

            // TODO
            if (prevChildPageID == null) { // 首次迭代
                prevChildPageID = internalPage.getChildPageIDFromLeft(0);
                if (prevChildPageID == null) {
                    return false;
                }
            }

            // TODO
            while (curIndex < internalPage.getMaxSlotNum()) {
                final int index = curIndex;
                curIndex++;
                Field key = internalPage.getKey(index);
                BTreePageID childPageId = internalPage.getChildPageIDFromLeft(index);
                if (key != null && childPageId != null) {
                    nextEntryToReturn = new BTreeEntry(key, prevChildPageID, childPageId);
                    nextEntryToReturn.setKeyItem(new KeyItem(internalPage.pageID, index));
                    prevChildPageID = childPageId;
                    return true;
                }
            }

            return false;
        }

        public BTreeEntry next() {
            BTreeEntry next = nextEntryToReturn;
            if (next == null) {
                if (hasNext()) {
                    next = nextEntryToReturn;
                    nextEntryToReturn = null;
                    return next;
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                nextEntryToReturn = null;
                return next;
            }
        }
    }

    /**
     * 反向迭代器
     */
    public class BTreeInternalPageReverseIterator implements Iterator<BTreeEntry> {
        /**
         * 当前遍历的key和children的下标
         */
        int curIndex;

        /**
         * 暂存上一次取得的childPageID，在构造BtreeEntry时做临时变量使用
         */
        // BTreePageID prevChildPageID = null;

        /**
         * 做临时变量使用，在next()中返回
         */
        BTreeEntry nextEntryToReturn = null;

        BTreeInternalPage internalPage;


        /**
         * 暂存迭代器访问的前一个子PageID，在构造BtreeEntry时做临时变量使用
         */
        BTreePageID reversePrevChildPageID = null;

        /**
         * 索引值
         */
        Field key = null;

        /**
         * 暂存迭代器访问的前一个keyItem，在构造BtreeEntry时做临时变量使用
         */
        KeyItem keyItem = null;


        public BTreeInternalPageReverseIterator(BTreeInternalPage internalPage) {
            this.internalPage = internalPage;
            this.curIndex = getMaxSlotNum() - 1;//entry个数

            // 将游标指向最右侧的不为空的entry
            while (!isSlotUsed(curIndex) && curIndex > 0) {
                curIndex--;
            }
        }

        public boolean hasNext() {
            if (nextEntryToReturn != null) {
                return true;
            }

            // 首次访问
            if (reversePrevChildPageID == null
                    || key == null
                    || keyItem == null) {
                reversePrevChildPageID = internalPage.getChildPageIDFromRight(curIndex);
                key = internalPage.getKey(curIndex);
                keyItem = new KeyItem(internalPage.getPageID(), curIndex);

                if (reversePrevChildPageID == null || key == null) {
                    return false;
                }
            }

            // 遍历 [lastSlot,1]
            while (curIndex > 0) {
                --curIndex;
                int index = curIndex;
                BTreePageID childPageID = internalPage.getChildPageIDFromRight(index);

                // 如果找到子pageID，封装entry
                if (childPageID != null) {
                    nextEntryToReturn = new BTreeEntry(key, childPageID, reversePrevChildPageID);
                    nextEntryToReturn.setKeyItem(keyItem);
                    reversePrevChildPageID = childPageID;
                    key = internalPage.getKey(index);
                    keyItem = new KeyItem(internalPage.getPageID(), index);
                    return true;
                }
            }
            return false;
        }

        public BTreeEntry next() {
            BTreeEntry next = nextEntryToReturn;
            if (next == null) {
                if (hasNext()) {
                    next = nextEntryToReturn;
                    nextEntryToReturn = null;
                    return next;
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                nextEntryToReturn = null;
                return next;
            }
        }
    }
}
