package com.microdb.model.page.bptree;

import com.microdb.model.page.PageID;

import java.io.Serializable;
import java.util.Objects;

/**
 * B+Tree pageID
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreePageID implements PageID, Serializable {

    private static final long serialVersionUID = -4673705142358467380L;
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
     * @see BPTreePageType
     */
    private int pageType;

    public BPTreePageID(int tableId, int pageNo, int pageType) {
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
        BPTreePageID that = (BPTreePageID) o;
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
        return "BPTreePageID{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                ", pageType=" + pageType +
                '}';
    }
}
