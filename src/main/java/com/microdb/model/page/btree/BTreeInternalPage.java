package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.field.FieldType;
import com.microdb.model.page.Page;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * BTreeInternalPage 存储索引Key
 * 格式：slotBitMap + 索引键值（m+1个位置，位置0不存储）+子节点指针(m+1个)+子节点page类型
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeInternalPage extends BTreePage {

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
    private Field[] keyList;

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

    private TableDesc tableDesc;

    /**
     * 索引字段在表结构中的下标
     */
    private int keyFieldIndex;

    /**
     * 一页数据最多可存放的节点元素数量
     */
    private int maxSlotNum;

    public BTreeInternalPage() {
        this.maxSlotNum = calculateMaxSlotNum(tableDesc);
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

    @Override
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
        if (index < 0 || index >= keyList.length) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        return keyList[index];
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
            keyList[1] = entry.getKey(); // keyList 0位置不存放数据
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
        keyList[matchedSlot] = entry.getKey();
        childrenPageNos[matchedSlot] = entry.getRightChild().getPageNo();
        entry.setKeyItem(new KeyItem(pageID, matchedSlot));
    }

    private void shift(int from, int to) {
        if (!isSlotUsed(to) && isSlotUsed(from)) {
            markSlotUsed(to, true);
            keyList[to] = keyList[from];
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
        markSlotUsed(keyItem.getKeyItemNo(), false);
        entry.setKeyItem(null);
    }

    private void markSlotUsed(int keyItemNo, boolean isUsed) {
        slotUsageStatusBitMap[keyItemNo] = isUsed;
    }
    // =====================================迭代器==========================================

    /**
     * 查找B+树内部节点的迭代器
     */
    static class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
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
