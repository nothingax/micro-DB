package com.microdb.model.page.btree;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * BTreeInternalPage 存储索引Key
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeInternalPage implements Page {

    /**
     * 该页的pageID
     */
    private BTreePageID pageID;

    /**
     * 所有子节点的page编号
     * 本页中有 m 个key，则对应的子节点有m+1个
     * size size为key的数量+1
     */
    private List<Integer> childrenPageNos;

    /**
     * 存储本页中的key
     * keyList size为key的数量+1, index 0 不存储内容
     */
    private List<Field> keyList;

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

    @Override
    public PageID getPageID() {
        return null;
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

    /**
     * 页中第index个槽位是否存在内容
     */
    @Override
    public boolean isSlotUsed(int index) {
        return slotUsageStatusBitMap[index];
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

    /**
     * 获取第index个子节点pageID
     * 通过页内存储的子节点的pageNo，构造并返回pageID
     */
    private BTreePageID getChildPageID(int index) {
        if (index < 0 || index >= childrenPageNos.size()) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(), childrenPageNos.get(index), childrenPageType);
    }

    /**
     * 获取第index个节点的键值
     */
    protected Field getKey(int index) {
        if (index < 0 || index >= keyList.size()) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        return keyList.get(index);
    }

    /**
     * 返回该页的迭代器
     */
    public BTreeInternalPageIterator getIterator() {
        return new BTreeInternalPageIterator(this);
    }

    public void insertEntry(BTreeEntry entry) {

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
}
