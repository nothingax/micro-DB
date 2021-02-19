package com.microdb.model.dbfile;

import com.microdb.model.page.Page;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 表磁盘文件，存储一个表的数据
 *
 * @author zhangjw
 * @version 1.0
 */
public class DbTableFile {

    /**
     * 文件
     */
    private File file;


    public DbTableFile(File file) {
        this.file = file;
    }

    /**
     * 按pageNo读取一页数据
     *
     * @param pageNo number of page
     * @return page
     */
    private Page readPageFromDisk(int pageNo) {
        byte[] pageData = new byte[Page.defaultPageSizeByte];
        try {
            FileInputStream in = new FileInputStream(file);
            in.skip(pageNo * Page.defaultPageSizeByte);
            in.read(pageData);
        } catch (IOException e) {
            throw new RuntimeException("todo ,read Page from disk error", e);
        }
        return new Page(pageNo, pageData);
    }

    /**
     * 写数据至磁盘
     *
     * @param page
     */
    public void writePageToDisk(Page page) {
        byte[] pgData = page.getPageData();
        try {
            RandomAccessFile dbFile = new RandomAccessFile(file, "rws");
            dbFile.skipBytes(page.getPageNo() * Page.defaultPageSizeByte);
            dbFile.write(pgData);
        } catch (IOException e) {
            throw new RuntimeException("todo ,write Page To Disk error", e);
        }
    }

    /**
     * 返回文件的唯一id
     *
     * @return  表文件的唯一标识
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

}
