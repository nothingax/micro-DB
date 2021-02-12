package com.microdb.model.page;

import com.microdb.model.Tuple;

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
    public static Integer defaultPageSizeByte = 4096;

    /**
     * 页号
     */
    private int pageNo;

    /**
     * 当前page中的元组
     */
    private Tuple[] tuples;

    /**
     * 每页的数据
     */
    private byte[] pageData;

    public int getPageNo() {
        return pageNo;
    }

    public byte[] getPageData() {
        return pageData;
    }

    public Page(int pageNo, byte[] pageData) {
        this.pageNo = pageNo;
        this.pageData = pageData;
    }
}
