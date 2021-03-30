package com.microdb.logging;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.page.Page;
import com.microdb.transaction.TransactionID;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;

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

    /**
     * long 长度
     */
    private int long_size = 8;

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

    /**
     * 将事务修改过的页面，在磁盘刷回原始版本，缓存中丢弃
     *
     * @param transactionID 回滚的事务
     */
    public synchronized void rollback(TransactionID transactionID) {
        try {
            // TODO 页大小并不固定，需要根据记录的偏移量来取
            raf.seek(raf.length() - long_size);
            long location = raf.readLong();
            raf.seek(raf.length() - long_size - Page.defaultPageSizeInByte - long_size);
            long lastTid = raf.readLong();
            while (transactionID.serialize() <= lastTid) {
                if (transactionID.serialize() == lastTid) {
                    // TODO 反序列化，log存储时，需要存储序列化class名等
                    byte[] bytes = new byte[Page.defaultPageSizeInByte];
                    raf.read(bytes);
                    Page beforePage = null;
                    DbTable dbTableById = DataBase.getInstance().getDbTableById(beforePage.getPageID().getTableId());
                    dbTableById.getTableFile().writePageToDisk(beforePage);
                    DataBase.getBufferPool().discardPages(Collections.singletonList(beforePage.getPageID()));
                }

                raf.seek(location - long_size);
                location = raf.readLong();
                raf.seek(location - long_size - Page.defaultPageSizeInByte - long_size);
                lastTid = raf.readLong();
            }
            // 复位
            raf.seek(offset);
        } catch (IOException e) {
            throw new DbException("undo file rollback failed", e);
        }
    }


}
