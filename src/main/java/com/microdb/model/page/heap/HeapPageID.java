package com.microdb.model.page.heap;

import com.microdb.model.page.PageID;

import java.util.Objects;

/**
 * pageID用于封装页编号、引用tableId，便于在page中使用tableDesc
 *
 * @author zhangjw
 * @version 1.0
 */
public class HeapPageID implements PageID {
    /**
     * 表Id
     */
    private int tableId;

    /**
     * 页编号
     */
    private int pageNo;

    public HeapPageID(int tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeapPageID that = (HeapPageID) o;
        return tableId == that.tableId &&
                pageNo == that.pageNo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNo);
    }

    @Override
    public String toString() {
        return "HeapPageID{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                '}';
    }
}
