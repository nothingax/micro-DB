package com.microdb.logging;

import com.microdb.model.page.Page;
import com.microdb.transaction.TransactionID;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * 日志文件
 *
 * @author zhangjw
 * @version 1.0
 */
public class UndoLogFile {
    /**
     * 文件
     */
    private final File file;

    /**
     * 随机存储文件
     */
    private RandomAccessFile raf;

    public UndoLogFile(File file) throws FileNotFoundException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
    }

    /**
     * 写入更新日志
     *
     * @param transactionID
     * @param beforePage
     * @param afterPage
     */
    public synchronized void writeUpdateLog(TransactionID transactionID,
                                            Page beforePage,
                                            Page afterPage) {

    }


}
