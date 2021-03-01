package com.microdb.model.dbfile;

import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import com.microdb.operator.ITableFileIterator;

import java.io.IOException;

/**
 * 表磁盘文件，存储一个表的数据
 *
 * @author zhangjw
 * @version 1.0
 */
public interface TableFile {


    /**
     * 获取该表的结构
     */
    TableDesc getTableDesc();

    /**
     * 读取一页数据
     *
     * @param pageID pageID
     * @return page
     */
    Page readPageFromDisk(PageID pageID) throws IOException;

    /**
     * 写数据至磁盘
     */
    void writePageToDisk(Page page);

    /**
     * 返回文件的唯一id
     * 取文件绝对路径散列值
     */
    int getTableId();

    /**
     * 文件中已存在的page数量
     */
    int getExistPageCount();

    void insertRow(Row row) throws IOException;

    void deleteRow(Row row);

    ITableFileIterator getIterator();
}
