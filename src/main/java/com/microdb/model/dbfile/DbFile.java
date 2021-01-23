package com.microdb.model.dbfile;

import com.microdb.model.page.Page;

import java.io.File;

/**
 * 表磁盘文件，存储一个表的数据
 *
 * @author zhangjw
 * @version 1.0
 */
public class DbFile {

    /**
     * 文件
     */
    private File file;


    public DbFile(File file) {
        this.file = file;
    }

    /**
     * 按pageNo读取一页数据
     *
     * @param pageNo number of page
     * @return page
     */
    private Page readPage(int pageNo) {

        // TODO
        return null;
    }

    /**
     * 写入一页数据
     *
     * @param page
     */
    private void writePage(Page page) {

        // TODO
    }
}
