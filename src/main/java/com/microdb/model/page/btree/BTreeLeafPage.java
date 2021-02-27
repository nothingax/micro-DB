package com.microdb.model.page.btree;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import com.microdb.operator.PredicateEnum;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * B+ Tree 叶页
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeLeafPage implements Page {

    /**
     * 该页的左右兄弟page指针、父page指针
     */
    public static final int POINTER_SIZE_IN_BYTE = 4;

    /**
     * 该页ID
     */
    private BTreePageID pageID;

    /**
     * 结构
     */
    private TableDesc tableDesc;

    /**
     * 键在表结构中的下标
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


    /**
     * 左兄弟
     */
    private int leftSibling;

    /**
     * 右兄弟
     */
    private int rightSibling;


    public TableDesc getTableDesc() {

        return null;
    }


    public BTreeLeafPage(BTreePageID pageID, int keyFieldIndex, byte[] pageData) throws IOException {
        this.pageID = pageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableDesc();
        this.keyFieldIndex = keyFieldIndex;
        this.maxSlotNum = 0;// TODO 获取最大承载量
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

    @Override
    public Iterator<Row> getRowIterator() {
        return null;
    }
}
