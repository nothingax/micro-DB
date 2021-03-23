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

    public class ChildPages {
        int leftPageNo;
        int rightPageNo;

        public ChildPages() {
        }

        // public ChildPages(int leftPageNo, int rightPageNo) {
        //     this.leftPageNo = leftPageNo;
        //     this.rightPageNo = rightPageNo;
        // }

        public ChildPages(BTreeEntry entry) {
            this.leftPageNo = entry.getLeftChildPageID().getPageNo();
            this.rightPageNo = entry.getRightChildPageID().getPageNo();
        }

        BTreePageID toPageID(int pageNo) {
            return new BTreePageID(pageID.getTableId(), pageNo, childrenPageType);
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


    /**
     * 初始化一块leafPae页面默认大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[Page.defaultPageSizeInByte];
    }

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

        // 第一个节点特殊处理
        if (getExistCount() == 0) {
            setEntry(entry, 0);
            return;
        }

        // 找到第一个空位置
        int firstEmptySlot = getFirstEmptySlot();
        if (firstEmptySlot == -1) {
            throw new DbException("insert entry error :no empty slot");
        }

        int first = getFirstKey();
        int last = getLastKey();
        if (entry.getKey().compare(PredicateEnum.LESS_THAN_OR_EQ, keys[first])) {
            if (first == 0) {
                for (int i = firstEmptySlot - 1; i >= 0; i--) {
                    shift(i, i + 1);
                }

                setEntry(entry, 0);

                int after = findEntryAfter(0);
                childPages[after].leftPageNo = childPages[0].rightPageNo;
            } else {
                setEntry(entry, 0);
                int after = findEntryAfter(0);
                childPages[after].leftPageNo = childPages[0].rightPageNo;
            }
        } else if (entry.getKey().compare(PredicateEnum.GREATER_THAN_OR_EQ, keys[last])) {
            if (last == maxSlotNum - 1) { // 没有位置
                // 左边有空位，将(firstEmptySlot,leftInsertIndex]向左平移1位
                for (int i = firstEmptySlot + 1; i <= last; i++) {
                    shift(i, i - 1);
                }
                setEntry(entry, last);
                int entryBefore = findEntryBefore(last);
                childPages[entryBefore].rightPageNo = childPages[last].leftPageNo;
            } else {
                int insertIndex = last + 1;
                setEntry(entry, insertIndex);
                childPages[last].rightPageNo = childPages[insertIndex].leftPageNo;
            }
        } else { // entry in (first ,last )
            int firstLessOrEq = findFirstLessOrEqIndex(entry.getKey());
            if (firstLessOrEq == -1) {
                throw new DbException("未找到第一个小于待插入Entry的key");
            }
            if (firstEmptySlot < firstLessOrEq) {
                // 向左平移1位，包括slotIndex ，元素插在slotIndex
                for (int i = firstEmptySlot + 1; i <= firstLessOrEq; i++) {
                    shift(i, i - 1);
                }
                setEntry(entry, firstLessOrEq);
                updateChild(entry, firstLessOrEq);

            } else { // 右边有空位 向右平移1，不包括slotIndex，元素插在slotIndex+1
                for (int i = firstEmptySlot - 1; i > firstLessOrEq; i--) {
                    shift(i, i + 1);
                }
                setEntry(entry, firstLessOrEq + 1);
                updateChild(entry, firstLessOrEq + 1);
            }
        }
    }

    private void updateChild(BTreeEntry entry, int slotIndex) {
        // 更新子页
        int before = findEntryBefore(slotIndex);
        if (before == -1) {
            throw new DbException("should not happen");
        }
        int after = findEntryAfter(slotIndex);
        if (after == -1) {
            throw new DbException("should not happen");
        }
        if (childPages[before].rightPageNo == entry.getLeftChildPageID().getPageNo()) {
            childPages[after].leftPageNo = entry.getRightChildPageID().getPageNo();
        } else if (childPages[after].leftPageNo == entry.getRightChildPageID().getPageNo()) {
            childPages[before].rightPageNo = entry.getLeftChildPageID().getPageNo();
        } else {
            throw new DbException("error");
        }
    }

    private void setEntry(BTreeEntry entry, int insertIndex) {
        markSlotUsed(insertIndex, true);
        keys[insertIndex] = entry.getKey();
        childPages[insertIndex] = new ChildPages(entry);
        entry.setKeyItem(new KeyItem(pageID, insertIndex));
    }

    private int findFirstLessOrEqIndex(Field key) {
        int lastUsed = -1;
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            if (isSlotUsed(i)) {
                if (keys[i].compare(PredicateEnum.GREATER_THAN_OR_EQ, key)) {
                    return lastUsed;
                }
                lastUsed = i;
            }
        }
        return -1;
    }

    private int getLastKey() {
        for (int i = slotUsageStatusBitMap.length - 1; i >= 0; i--) {
            if (isSlotUsed(i)) {
                return i;
            }
        }
        return -1;
    }

    private int getFirstKey() {
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            if (isSlotUsed(i)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 查找左侧使用的槽位下标
     */
    private int findEntryBefore(int slot) {
        for (int i = slot - 1; i >= 0; i--) {
            if (slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 查找右侧使用的槽位下标，包括slot本身
     */
    private int findEntryAfterClosed(int slot) {
        for (int i = slot; i < slotUsageStatusBitMap.length; i++) {
            if (slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }




    private int findEntryAfter(int matchedSlot) {
        for (int i = matchedSlot + 1; i < slotUsageStatusBitMap.length; i++) {
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

    private BTreeEntry getEntry(int index) {
        if (index < 0 || !isSlotUsed(index)) {
            // throw new DbException(String.format("不存在索引为%s的Entry", index));
            return null;
        }
        BTreePageID leftPageID = new BTreePageID(this.pageID.getTableId(), childPages[index].leftPageNo, childrenPageType);
        BTreePageID rightPageID = new BTreePageID(this.pageID.getTableId(), childPages[index].rightPageNo, childrenPageType);
        BTreeEntry entry = new BTreeEntry(keys[index], leftPageID, rightPageID);
        entry.setKeyItem(new KeyItem(this.pageID, index));
        return entry;
    }
    // =====================================迭代器==========================================

    /**
     * 查找B+树内部节点的迭代器
     */
    public class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
        /**
         * 当前遍历的下标
         */
        int curIndex = 0;

        BTreeInternalPage internalPage;

        public BTreeInternalPageIterator(BTreeInternalPage internalPage) {
            this.internalPage = internalPage;
            curIndex = findEntryAfterClosed(curIndex);
        }

        public boolean hasNext() {
            return curIndex != -1;
        }

        public BTreeEntry next() {
            if (curIndex == -1) {
                throw new NoSuchElementException();
            }

            BTreeEntry entry = getEntry(curIndex);
            curIndex = findEntryAfter(curIndex);
            return entry;
        }
    }

    /**
     * 反向迭代器
     */
    public class BTreeInternalPageReverseIterator implements Iterator<BTreeEntry> {
        /**
         * 当前遍历的下标
         */
        int curIndex;

        BTreeInternalPage internalPage;

        public BTreeInternalPageReverseIterator(BTreeInternalPage internalPage) {
            this.internalPage = internalPage;
            this.curIndex = getMaxSlotNum() - 1;
            // 将游标指向最右侧的不为空的entry
            while (!isSlotUsed(curIndex) && curIndex > 0) {
                curIndex--;
            }
        }

        public boolean hasNext() {
            return getEntry(curIndex) != null;
        }

        public BTreeEntry next() {
            if (curIndex == -1) {
                throw new NoSuchElementException();
            }
            BTreeEntry entry = getEntry(curIndex);
            curIndex = findEntryBefore(curIndex);
            return entry;
        }
    }
}
