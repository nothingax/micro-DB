package com.microdb.model.page.heap;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 页，读写磁盘文件数据时以page为基本单位
 *
 * @author zhangjw
 * @version 1.0
 */
public class HeapPage implements Page {
    /**
     * 默认每页4KB
     */
    public static int defaultPageSizeInByte = 4096;
    /**
     * page 编号
     */
    private PageID pageID;
    /**
     * 行数据数组
     */
    private Row[] rows;
    /**
     * 表结构
     */
    private TableDesc tableDesc;
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


    @Override
    public int getPageNo() {
        return pageID.getPageNo();
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(HeapPage.defaultPageSizeInByte);
        DataOutputStream dos = new DataOutputStream(baos);

        // 1.序列化 slot 使用状态位图
        for (boolean status : slotUsageStatusBitMap) {
            dos.writeBoolean(status);
        }

        // 2.序列化行数据
        for (int i = 0; i < rows.length; i++) {
            if (isSlotUsed(i)) {
                for (Field field : rows[i].getFields()) {
                    field.serialize(dos);
                }
            } else { // 空slot的位置填充
                dos.write(new byte[tableDesc.getRowMaxSizeInBytes()]);
            }
        }

        // 3.slot状态位图和行数据之外的位置填充 0
        int zeroSize =
                HeapPage.defaultPageSizeInByte
                        - slotUsageStatusBitMap.length
                        - tableDesc.getRowMaxSizeInBytes() * rows.length;
        byte[] zeroes = new byte[zeroSize];
        dos.write(zeroes, 0, zeroSize);

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * 反序列化文件数据到page
     * 将 slotUsageStatusBitMap 、rows 字节反序列化到对象
     */
    private void deserialize(byte[] pageData) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(pageData));
        // slot状态位反序列化
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = dis.readBoolean();
        }
        // 行数据Row反序列化
        rows = new Row[maxSlotNum];
        for (int i = 0; i < rows.length; i++) {
            if (isSlotUsed(i)) {
                Row row = new Row(this.tableDesc);
                Field[] fields = this.tableDesc.getFieldTypes()
                        .stream()
                        .map(x -> x.parse(dis))
                        .toArray(Field[]::new);
                row.setFields(fields);
                rows[i] = row;
            } else {
                rows[i] = null;
            }
        }

        dis.close();
    }

    private boolean isSlotUsed(int i) {
        return slotUsageStatusBitMap[i];
    }

    private boolean isSlotEmpty(int i) {
        return !slotUsageStatusBitMap[i];
    }

    public HeapPage(PageID pageID, byte[] pageData) throws IOException {
        this.pageID = pageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableDesc();
        this.maxSlotNum = calculateMaxSlotNum(this.tableDesc);
        this.rows = new Row[this.maxSlotNum];
        this.slotUsageStatusBitMap = new boolean[this.maxSlotNum];
        deserialize(pageData);
    }

    public static byte[] createEmptyPageData() {
        return new byte[HeapPage.defaultPageSizeInByte];
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
     * 计算返回一页数据可存放的数据行数
     * 页字节数容量（4KB）/(表一行占用字节+行的状态标识占用字节），向下取整
     * 行的状态标识占用位数：每行占用1byte
     */
    @Override
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        // slot状态位占用空间=1byte
        int slotStatusSizeInByte = 1;
        return HeapPage.defaultPageSizeInByte / (tableDesc.getRowMaxSizeInBytes() + slotStatusSizeInByte);
    }

    @Override
    public void insertRow(Row row) {
        if (row == null) {
            throw new DbException("insert row error: row can not be null");
        }
        for (int i = 0; i < this.maxSlotNum; i++) {
            if (!slotUsageStatusBitMap[i]) {
                slotUsageStatusBitMap[i] = true;
                rows[i] = row;
                return;
            }
        }
        throw new DbException("insert row error: no empty slot");
    }

    @Override
    public Iterator<Row> getRowIterator() {
        return new RowIterator();
    }

    //====================================迭代器======================================

    private class RowIterator implements Iterator<Row> {
        private Iterator<Row> rowIterator;
        public  RowIterator() {
            ArrayList<Row> rowList = new ArrayList<>();
            for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
                if (isSlotUsed(i)) {
                    rowList.add(rows[i]);
                }
            }
            rowIterator = rowList.iterator();
        }

        @Override
        public boolean hasNext() {
            return rowIterator.hasNext();
        }

        @Override
        public Row next() {
            return rowIterator.next();
        }
    }
}
