package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.page.Page;
import com.microdb.operator.PredicateEnum;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * B+ Tree 叶页
 * 格式：slotStatusBitMap + rows + 三个指针（父节点、左右、兄弟节点）
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeLeafPage extends BTreePage {

    /**
     * 该页的左右兄弟page指针、父page指针
     */
    public static final int POINTER_SIZE_IN_BYTE = 4;

    /**
     * 结构
     */
    private TableDesc tableDesc;

    /**
     * 索引字段在表结构中的下标
     */
    private int keyFieldIndex;

    /**
     * 一页数据最多可存放的数据行数
     */
    private int maxSlotNum;


    /**
     * slot使用状态标识位图
     * 为利用 {@link DataOutputStream#writeBoolean(boolean)} api的便利性，
     * 物理文件使用一个byte存储一个状态位
     */
    private boolean[] slotUsageStatusBitMap;

    /**
     * 行
     */
    private List<Row> rows;

    // /**
    //  * 父节点pageNo,可能是internal节点或者是rootPtr节点
    //  */
    // private int parentPageNo;

    /**
     * 左兄弟
     */
    private int leftSiblingPageNo;

    /**
     * 右兄弟
     */
    private int rightSiblingPageNo;


    public TableDesc getTableDesc() {

        return null;
    }


    public BTreeLeafPage(BTreePageID pageID, int keyFieldIndex, byte[] pageData) throws IOException {
        this.pageID = pageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableDesc();
        this.keyFieldIndex = keyFieldIndex;
        this.maxSlotNum = this.calculateMaxSlotNum(this.tableDesc);
        deserialize(pageData);
    }

    /**
     * 计算返回一页数据可存放的数据行数
     * 页字节数容量（4KB）/(表一行占用字节+行的状态标识占用字节），向下取整
     * 行的状态标识占用位数：每行占用1byte
     */

    /**
     * 初始化一块leafPae页面默认大小的空间
     */
    public static byte[] createEmptyPageData() {
        return new byte[Page.defaultPageSizeInByte];
    }

    @Override
    public BTreePageID getPageID() {
        return pageID;
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
        throw new UnsupportedOperationException("todo");
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
    @Override
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        // slot状态位占用空间=1byte
        int slotStatusSizeInByte = 1;
        int sizePerRowInBytes = tableDesc.getRowMaxSizeInBytes() + slotStatusSizeInByte;

        int pointerSizeInBytes = 3 * POINTER_SIZE_IN_BYTE;
        return (Page.defaultPageSizeInByte - pointerSizeInBytes) / sizePerRowInBytes;
    }

    @Override
    public int getMaxSlotNum() {
        return this.maxSlotNum;
    }

    public void insertRow(Row row) {
        // 校验表结构
        if (!Objects.equals(row.getTableDesc(), this.tableDesc)) {
            throw new DbException("tableDesc mismatch");
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
                if (rows.get(i).getField(keyFieldIndex).compare(PredicateEnum.LESS_THAN_OR_EQ, key)) {
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
        rows.set(matchedSlot, row);
    }

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }

    public Iterator<Row> getReverseIterator() {
        return new BTreeLeafPageReverseIterator(this);
    }

    /**
     * 当前页已经存储的行数
     */
    public int getExistRowCount() {
        int cnt = 0;

        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            cnt += slotUsageStatusBitMap[i] ? 1 : 0;
        }

        return cnt;
    }
    /**
     * 将from数据移动到to位置，清空from
     */
    private void shift(int from, int to) {
        if (!isSlotUsed(to) && isSlotUsed(from)) {
            slotUsageStatusBitMap[to] = true;
            rows.set(to, rows.get(from));
            slotUsageStatusBitMap[from] = false;
        }
    }

    private BTreePageID pageNoToPageID(int pageNo) {
        if (pageNo == 0) {
            return null;
        }
        return new BTreePageID(pageID.getTableId(), pageNo, BTreePageType.INTERNAL);
    }

    public BTreePageID getRightSibPageID() {
        return this.pageNoToPageID(rightSiblingPageNo);
    }

    public BTreePageID getLeftSibPageID() {
        return this.pageNoToPageID(leftSiblingPageNo);
    }

    public void setRightSibPageID(BTreePageID rightSibPageID) {
        if (rightSibPageID == null) {
            rightSiblingPageNo = 0;
        } else {
            if (rightSibPageID.getTableId() != pageID.getTableId()) {
                throw new DbException("tableID mismatch");
            }
            if (rightSibPageID.getPageType() != BTreePageType.LEAF) {
                throw new DbException("rightSibPage must be leaf");
            }
            rightSiblingPageNo = rightSibPageID.getPageNo();
        }
    }

    public void setLeftSibPageID(BTreePageID leftSibPageID) {
        if (leftSibPageID == null) {
            leftSiblingPageNo = 0;
        } else {
            if (leftSibPageID.getTableId() != pageID.getTableId()) {
                throw new DbException("tableID mismatch");
            }
            if (leftSibPageID.getPageType() != BTreePageType.LEAF) {
                throw new DbException("leftSibPage must be leaf");
            }
            leftSiblingPageNo = leftSibPageID.getPageNo();
        }
    }

    protected Row getRow(int index) {
        if (index < 0 || index >= rows.size()) {
            throw new NoSuchElementException(String.format("不存在索引为%s的元素", index));
        }
        if (!isSlotUsed(index)) {
            return null;
        }
        return rows.get(index);
    }

    /**
     * 删除行
     */
    public void deleteRow(Row row) {
        // TODO
    }

    // ===============================迭代器=========================================
    public static class BTreeLeafPageReverseIterator implements Iterator<Row> {
        int curIndex;
        Row nextRowToReturn = null;
        BTreeLeafPage leafPage;

        public BTreeLeafPageReverseIterator(BTreeLeafPage leafPage) {
            this.leafPage = leafPage;
            this.curIndex = leafPage.getMaxSlotNum() - 1;
        }

        public boolean hasNext() {
            if (nextRowToReturn != null) {
                return true;
            }

            try {
                while (curIndex >= 0) {
                    nextRowToReturn = leafPage.getRow(curIndex--);
                    if (nextRowToReturn != null) {
                        return true;
                    }
                }
                return false;
            } catch (NoSuchElementException e) {
                return false;
            }
        }

        public Row next() {
            Row next = nextRowToReturn;
            if (next == null) {
                if (hasNext()) {
                    next = nextRowToReturn;
                    nextRowToReturn = null;
                    return next;
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                nextRowToReturn = null;
                return next;
            }
        }
    }


}
