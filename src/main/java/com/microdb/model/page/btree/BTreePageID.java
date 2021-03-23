package com.microdb.model.page.btree;

import com.microdb.model.page.PageID;

import java.util.Objects;

/**
 * B+Tree pageID
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreePageID implements PageID {

    /**
     * 表ID
     */
    private int tableId;

    /**
     * page 编号
     */
    private int pageNo;

    /**
     * 页类型
     *
     * @see BTreePageType
     */
    private int pageType;

    public BTreePageID(int tableId, int pageNo, int pageType) {
        this.tableId = tableId;
        this.pageNo = pageNo;
        this.pageType = pageType;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }

    public int getPageType() {
        return pageType;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BTreePageID that = (BTreePageID) o;
        return tableId == that.tableId &&
                pageNo == that.pageNo &&
                pageType == that.pageType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNo, pageType);
    }

    @Override
    public String toString() {
        return "BTreePageID{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                ", pageType=" + pageType +
                '}';
    }
}
