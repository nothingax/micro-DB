package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.Page;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * BTreeInternalPage 存储索引Key
 * 格式：父指针、子页类型、slotUsageStatusBitMap、索引键值（m+1个位置，位置0不存储）、子节点指针(m+1个)
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeInternalPage extends BTreePage {

    /**
     * 指针长度，4个字节
     */
    private static final int INDEX_SIZE_IN_BYTE = FieldType.INT.getSizeInByte();

    /**
     * 所有子节点的page编号
     * 本页中有 m 个key，则对应的子节点有m+1个
     * size size为key的数量+1
     */
    private int[] childrenPageNos;

    /**
     * 存储本页中的key
     * keyList size为key的数量+1, index 0 不存储内容
     */
    private Field[] keys;

    /**
     * 子节点page类型
     */
    private int childrenPageType;

    /**
     * slot使用状态标识位图
     * 为利用 {@link DataOutputStream#writeBoolean(boolean)} api的便利性，
     * 物理文件使用一个byte存储一个状态位
     */
    private boolean[] slotUsageStatusBitMap;
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
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] pageData) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(pageData));
        // 1. parentPageNo
        parentPageNo = dis.readInt();
        // 2. childrenPageType
        childrenPageType = dis.readByte();
        // 3. slotUsageStatusBitMap
        slotUsageStatusBitMap = new boolean[maxSlotNum];
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = dis.readBoolean();
        }

        // 4. keys
        keys = new Field[maxSlotNum];
        // 0位置不存放元素
        keys[0] = null;
        FieldType keyFieldType = tableDesc.getFieldTypes().get(keyFieldIndex);
        for (int i = 1; i < keys.length; i++) {
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
        childrenPageNos = new int[maxSlotNum];
        for (int i = 0; i < childrenPageNos.length; i++) {
            if (isSlotUsed(i)) {
                childrenPageNos[i] = ((IntField) FieldType.INT.parse(dis)).getValue();
            } else {
                for (int i1 = 0; i1 < INDEX_SIZE_IN_BYTE; i1++) {
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
        return getMaxEntryNum(tableDesc) + 1;
    }

    private int getMaxEntryNum(TableDesc tableDesc) {
        int slotStatusSizeInByte = 1;
        int childSizeInByte = FieldType.INT.getSizeInByte();
        int keySizeInByte = tableDesc.getFieldTypes().get(keyFieldIndex).getSizeInByte();

        // 索引值+子节点指针+slot状态位
        int perEntrySizeInByte = keySizeInByte + childSizeInByte + slotStatusSizeInByte;

        // 每页一个父指针，m个entry有m+1个子节点指针，一个字节表示子page类型、一个额外的header使用1字节。
        int extra = 2 * FieldType.INT.getSizeInByte() + 1 + 1;
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
    private BTreePageID getChildPageID(int index) {
        if (index < 0 || index >= childrenPageNos.length) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }

        return new BTreePageID(pageID.getTableId(), childrenPageNos[index], childrenPageType);
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
    public BTreeInternalPageIterator getIterator() {
        return new BTreeInternalPageIterator(this);
    }

    /**
     * 找位置，移动数据空出位置，插入节点
     */
    public void insertEntry(BTreeEntry entry) {
        // 首次插入节点时更新
        if (childrenPageType == BTreePageType.ROOT_PTR) {
            childrenPageType = BTreePageType.LEAF;
        }

        // 第一个节点特殊处理
        if (getExistCount() == getMaxSlotNum()) {
            childrenPageNos[0] = entry.getLeftChildPageID().getPageNo();
            childrenPageNos[1] = entry.getRightChildPageID().getPageNo();
            keys[1] = entry.getKey(); // keyList 0位置不存放数据
            markSlotUsed(0, true);
            markSlotUsed(1, true);
            entry.setKeyItem(new KeyItem(pageID, 1));
            return;
        }

        // 找到第一个空位置，从1开始，因为0不存放数据
        int emptySlot = getFirstEmptySlot();
        if (emptySlot == -1) {
            throw new DbException("insert entry error :no empty slot");
        }


        // 查找本页中与带插入节点有相同左孩子或右孩子的节点。
        int lessOrEqKey = -1;
        for (int i = 0; i < getMaxSlotNum(); i++) {
            if (isSlotUsed(i)) {
                if (childrenPageNos[i] == entry.getLeftChildPageID().getPageNo()
                        || childrenPageNos[i] == entry.getRightChildPageID().getPageNo()) {
                    lessOrEqKey = i;
                    if (childrenPageNos[i] == entry.getRightChildPageID().getPageNo()) {
                        childrenPageNos[i] = entry.getLeftChildPageID().getPageNo();
                    }
                } else if (lessOrEqKey != -1) {
                    // 找到了位置跳出循环
                    break;
                }
            }
        }

        if (lessOrEqKey == -1) {
            throw new DbException("error");
        }

        // 移动节点中元素，空出一个新位置
        int matchedSlot = -1;
        if (emptySlot < lessOrEqKey) {
            for (int i = emptySlot; i < lessOrEqKey; i++) {
                shift(i + 1, i);
            }
            matchedSlot = lessOrEqKey;
        } else {
            for (int i = emptySlot; i > lessOrEqKey + 1; i--) {
                shift(i - 1, i);
            }
            matchedSlot = lessOrEqKey + 1;
        }

        // 将 entry 插入匹配的位置
        markSlotUsed(matchedSlot, true);
        keys[matchedSlot] = entry.getKey();
        childrenPageNos[matchedSlot] = entry.getRightChildPageID().getPageNo();
        entry.setKeyItem(new KeyItem(pageID, matchedSlot));
    }

    private void shift(int from, int to) {
        if (!isSlotUsed(to) && isSlotUsed(from)) {
            markSlotUsed(to, true);
            keys[to] = keys[from];
            childrenPageNos[to] = childrenPageNos[from];
            markSlotUsed(from, false);
        }
    }

    private int getFirstEmptySlot() {
        int emptySlot = -1;
        for (int i = 1; i < this.maxSlotNum; i++) {
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

    public void deleteEntryAndRightChild(BTreeEntry entry) {
        KeyItem keyItem = entry.getKeyItem();
        markSlotUsed(keyItem.getSlotIndex(), false);
        entry.setKeyItem(null);
    }

    public boolean isLessThanHalfFull() {
        return this.getExistCount() < this.getMaxSlotNum() / 2;
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
        childrenPageNos[slotIndex] = entry.getRightChildPageID().getPageNo();
        keys[slotIndex] = entry.getKey();
    }
    // =====================================迭代器==========================================

    /**
     * 查找B+树内部节点的迭代器
     */
    public static class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
        /**
         * 当前遍历的key和children的下标
         */
        int curIndex = 1;

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
            if (prevChildPageID == null) { // 首次迭代
                prevChildPageID = internalPage.getChildPageID(0);
                if (prevChildPageID == null) {
                    return false;
                }
            }

            while (true) {
                final int index = curIndex;
                Field key = internalPage.getKey(index);
                BTreePageID childPageId = internalPage.getChildPageID(index);
                if (key != null && childPageId != null) {
                    nextEntryToReturn = new BTreeEntry(key, prevChildPageID, childPageId);
                    nextEntryToReturn.setKeyItem(new KeyItem(internalPage.pageID, index));
                    prevChildPageID = childPageId;
                    return true;
                }

                curIndex++;
            }
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
        BTreePageID prevChildPageID = null;

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
                reversePrevChildPageID = internalPage.getChildPageID(curIndex);
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
                BTreePageID childPageID = internalPage.getChildPageID(index);

                // 如果找到子pageID，封装entry
                if (childPageID != null) {
                    nextEntryToReturn = new BTreeEntry(key, childPageID, reversePrevChildPageID);
                    nextEntryToReturn.setKeyItem(keyItem);
                    prevChildPageID = childPageID;
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
