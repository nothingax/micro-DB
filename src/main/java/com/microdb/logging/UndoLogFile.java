package com.microdb.logging;

import com.microdb.exception.DbException;
import com.microdb.model.page.Page;
import com.microdb.transaction.TransactionID;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

    /**
     * 当前写到的位置
     */
    private long offset = -1;

    public UndoLogFile(File file) throws FileNotFoundException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
    }

    /**
     * 写入更新日志
     * 计入事务ID+页原始数据
     *
     * @param transactionID 事务ID
     * @param beforePage    原始页
     */
    public synchronized void writeUpdateLog(TransactionID transactionID,
                                            Page beforePage) {

        try {
            // 事务ID
            raf.writeLong(transactionID.serialize());
            // 原始数据
            raf.write(beforePage.serialize());
            // 本次写入的起始位置
            raf.writeLong(offset);

            // 更新offset
            offset = raf.getFilePointer();

            raf.getChannel().force(true);
            System.out.println("写入undo log ,offset=" + offset);
        } catch (IOException e) {
            throw new DbException("write undo file failed", e);
        }
    }

}
