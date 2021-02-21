package com.microdb.model.dbfile;

import com.microdb.exception.DbException;
import com.microdb.model.Tuple;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

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
     * 存储表数据的物理文件
     */
    private File file;

    public DbTableFile(File file) {
        this.file = file;
    }

    /**
     * 读取一页数据
     *
     * @param pageID pageID
     * @return page
     */
    private Page readPageFromDisk(PageID pageID) throws IOException {
        byte[] pageData = Page.createEmptyPageData();
        try {
            FileInputStream in = new FileInputStream(file);
            in.skip(pageID.getPageNo() * Page.defaultPageSizeInByte);
            in.read(pageData);
        } catch (IOException e) {
            throw new RuntimeException("todo ,read Page from disk error", e);
        }
        return new Page(pageID, pageData);
    }

    /**
     * 写数据至磁盘
     *
     * @param page
     */
    public void writePageToDisk(Page page) {
        try {
            byte[] pgData = page.getPageData();
            RandomAccessFile dbFile = new RandomAccessFile(file, "rws");
            dbFile.skipBytes(page.getPageNo() * Page.defaultPageSizeInByte);
            dbFile.write(pgData);
        } catch (IOException e) {
            throw new DbException("write page To disk error", e);
        }
    }

    /**
     * 返回文件的唯一id
     * 取文件绝对路径散列值
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * 文件中已存在的page数量
     */
    public int getExistPageCount() {
        return (int) file.length() / Page.defaultPageSizeInByte;
    }

    public void insertRow(Tuple tuple) throws IOException {
        int existPageCount = this.getExistPageCount();
        Page availablePage = getFirstPageHasEmptySlot(existPageCount);

        // 现有所有页面均没有空slot,新建立一个页面
        if (null == availablePage) {
            int pageNo = existPageCount; // 由于pageNo从0开始
            PageID pageID = new PageID(this.getId(), pageNo);
            availablePage = new Page(pageID, Page.createEmptyPageData());
        }
        availablePage.insertTuple(tuple);
        this.writePageToDisk(availablePage);
    }

    private Page getFirstPageHasEmptySlot(int existPageCount) throws IOException {
        // 当前文件还没有插入数据，返回空
        if (existPageCount == 0) {
            return null;
        }

        for (int pageNo = 0; pageNo < existPageCount; pageNo++) {
            PageID pageID = new PageID(this.getId(), pageNo);
            Page pg = this.readPageFromDisk(pageID);
            if (pg.hasEmptySlot()) {
                return pg;
            }
        }
        return null;
    }

}
