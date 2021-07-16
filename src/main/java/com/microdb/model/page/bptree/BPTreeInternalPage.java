package com.microdb.model.page.bptree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.field.Field;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.row.Row;
import com.microdb.model.row.RowID;
import com.microdb.model.table.TableDesc;
import com.microdb.operator.PredicateEnum;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * BPTreeInternalPage 存储索引Key
 * 格式：槽位状态标记（每一个slot占用1Byte）、父指针(4Byte)、子页类型（1Byte）、
 * 索引键值（m+1个位置，位置0不存储）、子节点指针(m+1 Byte)
 * m个key
 * m个slot mByte
 * m个ChildPages ：2*m*4B
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeInternalPage extends BPTreePage implements Serializable {

    private static final long serialVersionUID = -375689079604630693L;

    public class ChildPages {
        int leftPageNo;
        int rightPageNo;

        public ChildPages() {
        }

        // public ChildPages(int leftPageNo, int rightPageNo) {
        //     this.leftPageNo = leftPageNo;
        //     this.rightPageNo = rightPageNo;
        // }

        public ChildPages(BPTreeEntry entry) {
            this.leftPageNo = entry.getLeftChildPageID().getPageNo();
            this.rightPageNo = entry.getRightChildPageID().getPageNo();
        }

        BPTreePageID toPageID(int pageNo) {
            return new BPTreePageID(pageID.getTableId(), pageNo, childrenPageType);
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
    public static byte[] createEmptyPageData(int pageSize) {
        return new byte[pageSize];
    }

    public BPTreeInternalPage(DataBase dataBase, BPTreePageID bpTreePageID, byte[] pageData, int keyFieldIndex) throws IOException {
        this.dataBase = dataBase;
        this.pageID = bpTreePageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(bpTreePageID.getTableId()).getTableDesc();
        this.maxSlotNum = calculateMaxSlotNum(tableDesc);
        this.keyFieldIndex = keyFieldIndex;
        deserialize(pageData);
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(dataBase.getDBConfig().getPageSizeInByte());
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
        int paddingLength = dataBase.getDBConfig().getPageSizeInByte()
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

        return (dataBase.getDBConfig().getPageSizeInByte() - extra) / perEntrySizeInByte;
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
     * 返回该页的迭代器
     */
    public Iterator<BPTreeEntry> getIterator() {
        return new BPTreeInternalPageIterator(this);
    }

    /**
     * 找位置，移动数据空出位置，插入节点
     */
    public void insertEntry(BPTreeEntry entry) {
        if (childrenPageType == BPTreePageType.ROOT_PTR) {
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

                int after = findNextUsedSlotExclusive(0);
                childPages[after].leftPageNo = childPages[0].rightPageNo;
            } else {
                setEntry(entry, 0);
                int after = findNextUsedSlotExclusive(0);
                childPages[after].leftPageNo = childPages[0].rightPageNo;
            }
        } else if (entry.getKey().compare(PredicateEnum.GREATER_THAN_OR_EQ, keys[last])) {
            if (last == maxSlotNum - 1) { // 没有位置
                // 左边有空位，将(firstEmptySlot,leftInsertIndex]向左平移1位
                for (int i = firstEmptySlot + 1; i <= last; i++) {
                    shift(i, i - 1);
                }
                setEntry(entry, last);
                int entryBefore = findPrevUsedSlotExclusive(last);
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

    private void updateChild(BPTreeEntry entry, int slotIndex) {
        // 更新子页
        int before = findPrevUsedSlotExclusive(slotIndex);
        if (before == -1) {
            throw new DbException("should not happen");
        }
        int after = findNextUsedSlotExclusive(slotIndex);
        if (after == -1) {
            throw new DbException("should not happen");
        }

        // 链接子页
        if (childPages[before].rightPageNo == entry.getLeftChildPageID().getPageNo()) {
            childPages[after].leftPageNo = entry.getRightChildPageID().getPageNo();
        } else if (childPages[after].leftPageNo == entry.getRightChildPageID().getPageNo()) {
            childPages[before].rightPageNo = entry.getLeftChildPageID().getPageNo();
        } else {
            throw new DbException("error");
        }
    }

    private void setEntry(BPTreeEntry entry, int insertIndex) {
        markSlotUsed(insertIndex, true);
        keys[insertIndex] = entry.getKey();
        childPages[insertIndex] = new ChildPages(entry);
        entry.setRowID(new RowID(pageID, insertIndex));
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
     * 查找左侧使用的槽位下标,不包括slot
     */
    private int findPrevUsedSlotExclusive(int slot) {
        return findPrevUsedSlotInclusive(slot - 1);
    }

    /**
     * 查找左侧使用的槽位下标,包括slot
     */
    private int findPrevUsedSlotInclusive(int slot) {
        for (int i = slot; i >= 0; i--) {
            if (slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 查找右侧使用的槽位下标，包括slot
     */
    private int findNextUsedSlotInclusive(int slot) {
        for (int i = slot; i < slotUsageStatusBitMap.length; i++) {
            if (slotUsageStatusBitMap[i]) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 查找右侧使用的槽位下标，不包括slot
     */
    private int findNextUsedSlotExclusive(int slot) {
        return findNextUsedSlotInclusive(slot + 1);
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

    public Iterator<BPTreeEntry> getReverseIterator() {
        return new BPTreeInternalPageReverseIterator(this);
    }

    /**
     * 从左端开始删除entry
     * 用于左侧页发起重分布，将右页数据挪到左页/与右页合并时使用
     */
    public void deleteEntryFromTheLeft(BPTreeEntry entry) {

        // 第一个或最后一个直接删除
        // if (slotIndexDeleted != findFirstUsedSlot() && slotIndexDeleted != findLastUsedSlot()) {
        //     // 删除entry，并连接子页
        //     int prevUsedSlotExclusive = findPrevUsedSlotExclusive(slotIndexDeleted);
        //     int pageNo = entry.getRightChildPageID().getPageNo();
        //
        //     if (prevUsedSlotExclusive != -1) {
        //         childPages[prevUsedSlotExclusive].rightPageNo = pageNo;
        //     }
        // }

        int slotIndexDeleted = entry.getRowID().getSlotIndex();
        int prevUsedSlotExclusive = findPrevUsedSlotExclusive(slotIndexDeleted);
        if (prevUsedSlotExclusive != -1) {
            childPages[prevUsedSlotExclusive].rightPageNo = entry.getRightChildPageID().getPageNo();
        }

        // delete
        slotUsageStatusBitMap[slotIndexDeleted] = false;
        keys[slotIndexDeleted] = null;
        childPages[slotIndexDeleted] = null;
    }

    /**
     * 用于两页合并时删除entry，处理entry和(entry+1)的子叶关系
     * left 和  right 两页合并，right页删除
     * entry.right 和(entry+1).left 删除，entry.left应赋值给（entry+1).left
     */
    public void deleteEntryAndRightChildPage(BPTreeEntry entry) {
        int slotIndexDeleted = entry.getRowID().getSlotIndex();
        int nextUsedSlotExclusive = findNextUsedSlotExclusive(slotIndexDeleted);

        if (nextUsedSlotExclusive != -1) {
            childPages[nextUsedSlotExclusive].leftPageNo = entry.getLeftChildPageID().getPageNo();
        }

        // delete
        slotUsageStatusBitMap[slotIndexDeleted] = false;
        keys[slotIndexDeleted] = null;
        childPages[slotIndexDeleted] = null;
    }

    /**
     * 从由右端开始删除entry
     * internal page 分裂时使用，leftPage 分裂时，从右端开始迁走数据
     */
    public void deleteEntryFromTheRight(BPTreeEntry entry) {
        deleteEntryAndRightChildPage(entry);

        // int slotIndexDeleted = entry.getRowID().getSlotIndex();
        // // 第一个或最后一个直接删除
        // if (slotIndexDeleted != findFirstUsedSlot() && slotIndexDeleted != findLastUsedSlot()) {
        //     // 删除entry，并连接子页
        //     int prevUsedSlotExclusive = findPrevUsedSlotExclusive(slotIndexDeleted);
        //     int nextUsedSlotExclusive = findNextUsedSlotExclusive(slotIndexDeleted);
        //     int pageNo = entry.getLeftChildPageID().getPageNo();
        //
        //     // if (prevUsedSlotExclusive != -1) {
        //     //     childPages[prevUsedSlotExclusive].rightPageNo = pageNo;
        //     // }
        //     if (nextUsedSlotExclusive != -1) {
        //         childPages[nextUsedSlotExclusive].leftPageNo = pageNo;
        //     }
        // }


        // int slotIndexDeleted = entry.getRowID().getSlotIndex();
        // int nextUsedSlotExclusive = findNextUsedSlotExclusive(slotIndexDeleted);
        // if (nextUsedSlotExclusive != -1) {
        //     childPages[nextUsedSlotExclusive].leftPageNo = entry.getLeftChildPageID().getPageNo();
        // }
        //
        // // delete
        // slotUsageStatusBitMap[slotIndexDeleted] = false;
        // keys[slotIndexDeleted] = null;
        // childPages[slotIndexDeleted] = null;
    }

    private int findLastUsedSlot() {
        for (int i = maxSlotNum - 1; i >= 0; i--) {
            if (isSlotUsed(i)) {
                return i;
            }
        }
        return -1;
    }

    private int findFirstUsedSlot() {
        for (int i = 0; i < this.maxSlotNum; i++) {
            if (isSlotUsed(i)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 页内剩余元素数量是否满足重分布条件
     */
    public boolean isNeedRedistribute() {
        if ((this.getMaxSlotNum() & 1) == 1) {
            return this.getExistCount() < (this.getMaxSlotNum() + 1) / 2;
        } else {
            return this.getExistCount() < this.getMaxSlotNum() / 2;
        }
    }


    /**
     * 剩余元素数量是否满足合并条件
     * 奇数 e.g. 5: <=2
     * 偶数 e.g. 6: <=3
     */
    public boolean isMeetMergeCount() {
        return this.getExistCount() <= (this.getMaxSlotNum()) / 2;
    }

    private void markSlotUsed(int index, boolean isUsed) {
        slotUsageStatusBitMap[index] = isUsed;
    }

    public boolean isEmpty() {
        return this.getExistCount() == 0;
    }

    /**
     * 更新entry
     */
    public void updateEntry(BPTreeEntry entry) {
        RowID rowID = entry.getRowID();

        // 校验
        if (rowID == null) {
            throw new DbException("tried to update entry with null rowID");
        }
        if (rowID.getPageID().getTableId() != pageID.getTableId()) {
            throw new DbException("table id not match");
        }

        if (rowID.getPageID().getPageNo() != pageID.getPageNo()) {
            throw new DbException("page no not match");
        }
        if (!isSlotUsed(rowID.getSlotIndex())) {
            throw new DbException("update entry error:slot is not used");
        }

        // todo 检验元素顺序

        int slotIndex = rowID.getSlotIndex();
        childPages[slotIndex] = new ChildPages(entry);
        keys[slotIndex] = entry.getKey();
    }

    private BPTreeEntry getEntry(int index) {
        if (index < 0 || !isSlotUsed(index)) {
            // throw new DbException(String.format("不存在索引为%s的Entry", index));
            return null;
        }
        BPTreePageID leftPageID = new BPTreePageID(this.pageID.getTableId(), childPages[index].leftPageNo, childrenPageType);
        BPTreePageID rightPageID = new BPTreePageID(this.pageID.getTableId(), childPages[index].rightPageNo, childrenPageType);
        BPTreeEntry entry = new BPTreeEntry(keys[index], leftPageID, rightPageID);
        entry.setRowID(new RowID(this.pageID, index));
        return entry;
    }
    // =====================================迭代器==========================================

    /**
     * 查找B+树内部节点的迭代器
     */
    public class BPTreeInternalPageIterator implements Iterator<BPTreeEntry> {
        /**
         * 当前遍历的下标
         */
        int curIndex = 0;

        BPTreeInternalPage internalPage;

        public BPTreeInternalPageIterator(BPTreeInternalPage internalPage) {
            this.internalPage = internalPage;
            curIndex = findNextUsedSlotInclusive(curIndex);
        }

        public boolean hasNext() {
            return curIndex != -1;
        }

        public BPTreeEntry next() {
            if (curIndex == -1) {
                throw new NoSuchElementException();
            }

            BPTreeEntry entry = getEntry(curIndex);
            curIndex = findNextUsedSlotExclusive(curIndex);
            return entry;
        }
    }

    /**
     * 反向迭代器
     */
    public class BPTreeInternalPageReverseIterator implements Iterator<BPTreeEntry> {
        /**
         * 当前遍历的下标
         */
        int curIndex;

        BPTreeInternalPage internalPage;

        public BPTreeInternalPageReverseIterator(BPTreeInternalPage internalPage) {
            this.internalPage = internalPage;
            // 从右往左，找到第一个使用的槽位
            curIndex = findPrevUsedSlotInclusive(getMaxSlotNum() - 1);
        }

        public boolean hasNext() {
            return curIndex != -1;
        }

        public BPTreeEntry next() {
            if (curIndex == -1) {
                throw new NoSuchElementException();
            }
            BPTreeEntry entry = getEntry(curIndex);
            curIndex = findPrevUsedSlotExclusive(curIndex);
            return entry;
        }
    }
}
