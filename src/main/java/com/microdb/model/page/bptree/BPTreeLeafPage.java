package com.microdb.model.page.bptree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.field.Field;
import com.microdb.model.page.Page;
import com.microdb.model.row.Row;
import com.microdb.model.row.RowID;
import com.microdb.model.table.TableDesc;
import com.microdb.operator.PredicateEnum;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * B+ Tree 叶页
 * 格式：slotStatusBitMap + 三个指针（父节点、左右、兄弟节点）+ rows
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeLeafPage extends BPTreePage implements Serializable {
    private static final long serialVersionUID = -7469925154877861517L;

    /**
     * 该页的左右兄弟page指针、父page指针
     */
    public static final int POINTER_SIZE_IN_BYTE = 4;

    /**
     * slot使用状态标识位图
     * 为利用 {@link DataOutputStream#writeBoolean(boolean)} api的便利性，
     * 物理文件使用一个byte存储一个状态位
     */
    private boolean[] slotUsageStatusBitMap;

    /**
     * 一页数据最多可存放的数据行数
     */
    private int maxSlotNum;

    /**
     * 行
     */
    private Row[] rows;

    /**
     * 左兄弟
     */
    private int leftSiblingPageNo;

    /**
     * 右兄弟
     */
    private int rightSiblingPageNo;

    /**
     * 结构
     */
    private TableDesc tableDesc;

    public BPTreeLeafPage(BPTreePageID pageID, byte[] pageData, int keyFieldIndex) throws IOException {
        this.pageID = pageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableDesc();
        this.keyFieldIndex = keyFieldIndex;
        this.maxSlotNum = this.calculateMaxSlotNum(this.tableDesc);
        deserialize(pageData);
        // if (pageID.getPageNo() > 2) {
        //     if (parentPageNo == 0) {
        //         throw new DbException("叶子页读入磁盘error: parent page_no should not be 0");
        //     }
        // }
        check(this);

        // 保留页原始数据
        saveBeforePage();
    }

    private void check(BPTreeLeafPage leafPage) {
        // bitmap中数量
        int existRowCount = leafPage.getExistRowCount();

        // 实际存储的数量
        Row[] rows = leafPage.rows;
        int cnt = 0;
        for (Row b : rows) {
            cnt += b != null ? 1 : 0;
        }
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            Row b = rows[i];
            if (b != null) {
                if (b.getRowID() == null) {
                    throw new DbException("rowID 不能为空");
                }
                if (b.getRowID().getSlotIndex() != i) {
                    throw new DbException("rowID与下标不匹配");
                }
            }
        }

        if (existRowCount != cnt) {
            throw new DbException("页状态不一致，bitmap与实际存储rows的数量不一致，pageID=" + leafPage.getPageID());
        }
    }

    public TableDesc getTableDesc() {
        return tableDesc;
    }

    /**
     * 初始化一块leafPae页面默认大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[DataBase.getDBConfig().getPageSizeInByte()];
    }

    @Override
    public BPTreePageID getPageID() {
        return pageID;
    }

    @Override
    public byte[] serialize() throws IOException {
        check(this);
        // print(this);

        // if (this.getPageID().getPageNo() > 2) {
        //     if (this.getParentPageID().getPageNo() == 0) {
        //         throw new DbException("leafPage 序列化写入磁盘: parent page_no should not be 0");
        //     }
        // }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(DataBase.getDBConfig().getPageSizeInByte());
        DataOutputStream dos = new DataOutputStream(baos);

        // 1. slotUsageStatusBitMap
        for (boolean b : slotUsageStatusBitMap) {
            dos.writeBoolean(b);
        }
        // 2. 父指针、左兄弟、右兄弟指针
        dos.writeInt(parentPageNo);
        dos.writeInt(leftSiblingPageNo);
        dos.writeInt(rightSiblingPageNo);

        // 3. rows 行数据
        for (int i = 0; i < rows.length; i++) {
            if (isSlotUsed(i)) {
                for (int j = 0; j < tableDesc.getAttributesNum(); j++) {
                    rows[i].getField(j).serialize(dos);
                }
            } else {
                fillBytes(dos, tableDesc.getRowMaxSizeInBytes());
            }
        }

        // 4. 填充剩余空间
        int slotSize = slotUsageStatusBitMap.length;
        int indexSize = 3 * 4;
        int rowSize = tableDesc.getRowMaxSizeInBytes() * rows.length;
        fillBytes(dos, DataBase.getDBConfig().getPageSizeInByte() - slotSize - indexSize - rowSize);

        dos.flush();
        return baos.toByteArray();
    }

    private void print(BPTreeLeafPage leafPage) {
        int pageNo = leafPage.getPageID().getPageNo();
        int parentPageNo = leafPage.getParentPageID().getPageNo();
        int existRowCount = leafPage.getExistRowCount();
        StringBuilder append = new StringBuilder().append("叶子写入磁盘：")
                .append("pageNo:")
                .append(pageNo)
                .append(",parentPageNo:")
                .append(parentPageNo)
                .append(",existRowCount:")
                .append(existRowCount);

        System.out.println(append.toString());
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
        this.parentPageNo = dis.readInt();

        // 3.leftSiblingPageNo
        this.leftSiblingPageNo = dis.readInt();

        // 4.rightSiblingPageNo
        this.rightSiblingPageNo = dis.readInt();

        // 5. rows
        int rowMaxSizeInBytes = tableDesc.getRowMaxSizeInBytes();
        rows = new Row[maxSlotNum];
        for (int i = 0; i < rows.length; i++) {
            if (isSlotUsed(i)) {
                Row row = new Row(tableDesc);
                List<Field> fieldList = tableDesc.getFieldTypes().stream()
                        .map(x -> x.parse(dis))
                        .collect(Collectors.toList());
                row.setFields(fieldList);
                row.setRowID(new RowID(pageID, i));
                rows[i] = row;
            } else {
                rows[i] = null;
                for (int i1 = 0; i1 < rowMaxSizeInBytes; i1++) {
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

    @Override
    public boolean isSlotUsed(int index) {
        return slotUsageStatusBitMap[index];
    }

    /**
     * 返回该页容纳的行数
     * 每行数据使用1 byte存储行的使用状态
     * page需要额外3个指针维护左右兄弟page，父page
     */
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        // slot状态位占用空间=1byte
        int slotStatusSizeInByte = 1;
        int sizePerRowInBytes = tableDesc.getRowMaxSizeInBytes() + slotStatusSizeInByte;

        int pointerSizeInBytes = 3 * POINTER_SIZE_IN_BYTE;
        return (DataBase.getDBConfig().getPageSizeInByte() - pointerSizeInBytes) / sizePerRowInBytes;
    }

    @Override
    public int getMaxSlotNum() {
        return this.maxSlotNum;
    }

    public void insertRow(Row row) {
        // 校验表结构
        if (!Objects.equals(row.getTableDesc(), this.tableDesc)) {
            throw new DbException("table desc mismatch");
        }

        // 查找空位
        int emptySlotIndex = -1;
        for (int i = 0; i < maxSlotNum; i++) {
            if (!isSlotUsed(i)) {
                emptySlotIndex = i;
                break;
            }
        }
        if (emptySlotIndex == -1) {
            throw new DbException("insert row failed : no empty slot");
        }

        // 找到页内最后一个<=待插入数据 的索引
        int lastLessOrEqKeyIndex = -1;
        Field key = row.getField(keyFieldIndex);
        for (int i = 0; i < maxSlotNum; i++) {
            if (isSlotUsed(i)) {
                if (rows[i].getField(keyFieldIndex).compare(PredicateEnum.LESS_THAN_OR_EQ, key)) {
                    lastLessOrEqKeyIndex = i;
                } else {
                    break;
                }
            }
        }

        // 移动元素将正确位置空出
        int matchedSlot = -1;
        if (emptySlotIndex < lastLessOrEqKeyIndex) { // 合适的位置在空位右边，将lastLessOrEqKeyIndex左移动
            for (int i = emptySlotIndex; i < lastLessOrEqKeyIndex; i++) {
                shift(i + 1, i);
            }
            matchedSlot = lastLessOrEqKeyIndex;
        } else { // 合适的位置在空位左边边，将lastLessOrEqKeyIndex 右移
            for (int i = emptySlotIndex; i > lastLessOrEqKeyIndex + 1; i--) {
                shift(i - 1, i);
            }
            matchedSlot = lastLessOrEqKeyIndex + 1;
        }

        // 将新数据插入到正确位置上
        slotUsageStatusBitMap[matchedSlot] = true;

        row.setRowID(new RowID(pageID, matchedSlot));
        rows[matchedSlot] = row;
    }

    public Iterator<Row> getRowIterator() {
        return new BPTreeLeafPageIterator(this);
    }

    @Override
    public Page getBeforePage() {
        try {
            return new BPTreeLeafPage(pageID, beforePageData,keyFieldIndex);
        } catch (IOException e) {
            throw new DbException("get before page error", e);
        }
    }

    public Iterator<Row> getReverseIterator() {
        return new BPTreeLeafPageReverseIterator(this);
    }

    /**
     * 页内剩余元素数量是否满足重分布条件
     */
    public boolean isNeedRedistribute() {
        if ((this.getMaxSlotNum() & 1) == 1) {
            return this.getExistRowCount() < (this.getMaxSlotNum() + 1) / 2;
        } else {
            return this.getExistRowCount() < this.getMaxSlotNum() / 2;
        }
    }

    /**
     * 剩余元素数量是否满足合并条件
     * 奇数 e.g. 5: <=2
     * 偶数 e.g. 6: <=3
     */
    public boolean isMeetMergeCount() {
        return this.getExistRowCount() <= (this.getMaxSlotNum()) / 2;
    }

    /**
     * 当前页已经存储的行数
     */
    public int getExistRowCount() {
        int cnt = 0;
        for (boolean b : slotUsageStatusBitMap) {
            cnt += b ? 1 : 0;
        }
        return cnt;
    }

    /**
     * 将from数据移动到to位置，清空from
     */
    private void shift(int from, int to) {
        if (!isSlotUsed(to) && isSlotUsed(from)) {
            slotUsageStatusBitMap[to] = true;
            rows[to] = rows[from];
            rows[to].setRowID(new RowID(pageID, to));

            // 清空from
            slotUsageStatusBitMap[from] = false;
            rows[from] = null;
        }
    }

    private BPTreePageID pageNoToPageID(int pageNo) {
        if (pageNo == 0) {
            return null;
        }
        return new BPTreePageID(pageID.getTableId(), pageNo, BPTreePageType.LEAF);
    }

    public BPTreePageID getRightSibPageID() {
        return this.pageNoToPageID(rightSiblingPageNo);
    }

    public BPTreePageID getLeftSibPageID() {
        return this.pageNoToPageID(leftSiblingPageNo);
    }

    public void setRightSibPageID(BPTreePageID rightSibPageID) {
        if (rightSibPageID == null) {
            rightSiblingPageNo = 0;
        } else {
            if (rightSibPageID.getTableId() != pageID.getTableId()) {
                throw new DbException("tableID mismatch");
            }
            if (rightSibPageID.getPageType() != BPTreePageType.LEAF) {
                throw new DbException("rightSibPage must be leaf");
            }
            rightSiblingPageNo = rightSibPageID.getPageNo();
        }
    }

    public void setLeftSibPageID(BPTreePageID leftSibPageID) {
        if (leftSibPageID == null) {
            leftSiblingPageNo = 0;
        } else {
            if (leftSibPageID.getTableId() != pageID.getTableId()) {
                throw new DbException("tableID mismatch");
            }
            if (leftSibPageID.getPageType() != BPTreePageType.LEAF) {
                throw new DbException("leftSibPage must be leaf");
            }
            leftSiblingPageNo = leftSibPageID.getPageNo();
        }
    }

    protected Row getRow(int index) {
        if (index < 0 || index >= rows.length) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        return rows[index];
    }

    /**
     * 删除行
     */
    public void deleteRow(Row row) {
        int slotIndex = row.getRowID().getSlotIndex();
        slotUsageStatusBitMap[slotIndex] = false;
        rows[slotIndex] = null;
        row.setRowID(null);
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

    // ===============================迭代器=========================================

    /**
     * 正向迭代器
     */
    public class BPTreeLeafPageIterator implements Iterator<Row> {
        int curIndex;
        Row nextRowToReturn = null;
        BPTreeLeafPage leafPage;


        public BPTreeLeafPageIterator(BPTreeLeafPage leafPage) {
            this.leafPage = leafPage;
            this.curIndex = 0;
            curIndex = findNextUsedSlotInclusive(curIndex);
        }

        public boolean hasNext() {
            return curIndex != -1;
        }

        public Row next() {
            if (curIndex == -1) {
                throw new NoSuchElementException();
            }
            nextRowToReturn = getRow(curIndex);
            curIndex = findNextUsedSlotExclusive(curIndex);
            return nextRowToReturn;
        }
    }

    /**
     * 反向迭代器
     */
    public class BPTreeLeafPageReverseIterator implements Iterator<Row> {
        int curIndex;
        Row nextRowToReturn = null;
        BPTreeLeafPage leafPage;

        public BPTreeLeafPageReverseIterator(BPTreeLeafPage leafPage) {
            this.leafPage = leafPage;
            this.curIndex = findPrevUsedSlotInclusive(leafPage.getMaxSlotNum() - 1);
        }

        public boolean hasNext() {
            return curIndex != -1;
        }

        public Row next() {
            if (curIndex == -1) {
                throw new NoSuchElementException();
            }
            nextRowToReturn = getRow(curIndex);
            curIndex = findPrevUsedSlotExclusive(curIndex);
            return nextRowToReturn;
        }
    }

}
