package com.microdb.model.page;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.TableDesc;
import com.microdb.model.Tuple;
import com.microdb.model.field.Field;

import java.io.*;

/**
 * 页，读写磁盘文件数据时以page为基本单位
 *
 * @author zhangjw
 * @version 1.0
 */
public class Page {
    /**
     * 默认每页4KB
     */
    public static int defaultPageSizeInByte = 4096;
    /**
     * page 编号
     */
    private PageID pageID;
    /**
     * 行数据数组，每个tuple一行数据
     */
    private Tuple[] tuples;
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


    public int getPageNo() {
        return pageID.getPageNo();
    }
    /**
     * 序列化page数据
     */
    public byte[] getPageData() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Page.defaultPageSizeInByte);
        DataOutputStream dos = new DataOutputStream(baos);

        // 1.序列化 slot 使用状态位图
        for (boolean status : slotUsageStatusBitMap) {
            dos.writeBoolean(status);
        }

        // 2.序列化行数据
        for (int i = 0; i < tuples.length; i++) {
            if (isSlotUsed(i)) {
                for (Field field : tuples[i].getFields()) {
                    field.serialize(dos);
                }
            } else { // 空slot的位置填充
                dos.write(new byte[tableDesc.getRowMaxSizeInBytes()]);
            }
        }

        // 3.slot状态位图和行数据之外的位置填充 0
        int zeroSize =
                Page.defaultPageSizeInByte
                        - slotUsageStatusBitMap.length
                        - tableDesc.getRowMaxSizeInBytes() * tuples.length;
        byte[] zeroes = new byte[zeroSize];
        dos.write(zeroes, 0, zeroSize);

        dos.flush();
        return baos.toByteArray();
    }

    private boolean isSlotUsed(int i) {
        return slotUsageStatusBitMap[i];
    }

    private boolean isSlotEmpty(int i) {
        return !slotUsageStatusBitMap[i];
    }

    public Page(PageID pageID, byte[] pageData) throws IOException {
        this.pageID = pageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableDesc();
        this.maxSlotNum = calculateMaxSlotNum(this.tableDesc);
        this.tuples = new Tuple[this.maxSlotNum];
        this.slotUsageStatusBitMap = new boolean[this.maxSlotNum];
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(pageData));

        // slot状态位反序列化
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = dis.readBoolean();
        }

        // 行数据Tuple反序列化
        tuples = new Tuple[maxSlotNum];
        for (int i = 0; i < tuples.length; i++) {
            if (isSlotUsed(i)) {
                Tuple tuple = new Tuple(this.tableDesc);
                Field[] fields = this.tableDesc.getFieldTypes()
                        .stream()
                        .map(x -> x.parse(dis))
                        .toArray(Field[]::new);
                tuple.setFields(fields);
                tuples[i] = tuple;
            } else {
                tuples[i] = null;
            }
        }

        dis.close();
    }

    public static byte[] createEmptyPageData() {
        return new byte[Page.defaultPageSizeInByte];
    }

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
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        // slot状态位占用空间=1byte
        int slotStatusSizeInByte = 1;
        return Page.defaultPageSizeInByte / (tableDesc.getRowMaxSizeInBytes() + slotStatusSizeInByte);
    }

    public void insertTuple(Tuple tuple) {
        if (tuple == null) {
            throw new DbException("insertTuple error: tuple can not be null");
        }
        for (int i = 0; i < this.maxSlotNum; i++) {
            if (!slotUsageStatusBitMap[i]) {
                slotUsageStatusBitMap[i] = true;
                tuples[i] = tuple;
                return;
            }
        }
        throw new DbException("insertTuple error: no empty slot");
    }
}
