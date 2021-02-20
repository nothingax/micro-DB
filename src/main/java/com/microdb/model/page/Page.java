package com.microdb.model.page;

import com.microdb.exception.DbException;
import com.microdb.model.TableDesc;
import com.microdb.model.Tuple;
import com.microdb.model.field.Field;

import java.io.*;

/**
 * 页，读取磁盘文件数据时以page为基本单位
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
     * 页号
     */
    private int pageNo;


    /**
     * 页面中数据
     */
    private Tuple[] tuples;

    // /**
    //  * 每页的数据
    //  */
    // private byte[] pageData;

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
     */
    private boolean[] slotUsageStatusBitMap;

    public int getMaxSlotNum() {
        return maxSlotNum;
    }

    public int getPageNo() {
        return pageNo;
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
                        - ((slotUsageStatusBitMap.length + 7) / 8)
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

    public Page(int pageNo, byte[] pageData) throws IOException {
        this.pageNo = pageNo;
        // this.pageData = pageData;
        this.maxSlotNum = calculateMaxSlotNum();
        this.slotUsageStatusBitMap = new boolean[this.maxSlotNum];

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(pageData));

        // TODO 存在字节对齐问题
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = dis.readBoolean();
        }
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
     * 页位数容量/(表一行占用位数+行的状态标识占用位数），向下取整
     * 行的状态标识占用位数：每行占用1bit
     */
    public int calculateMaxSlotNum() {
        int defaultPageSizeInBit = Page.defaultPageSizeInByte * 8;
        int rowMaxSizeInBit = tableDesc.getRowMaxSizeInBytes() * 8;
        /**
         * slot 状态位占用空间
         */
        int slotStatusSizeInBit = 1;
        return (defaultPageSizeInBit) / (rowMaxSizeInBit * 8 + slotStatusSizeInBit);
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
