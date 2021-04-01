package com.microdb.logging;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.page.Page;
import com.microdb.transaction.TransactionID;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;

/**
 * undo 日志，用于回滚
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
     * 存储偏移地址占用的字节数，以long存储，占用8字节
     */
    private int offsetAddrInByte = 8;

    /**
     * 事务开始时的偏移地址
     */
    HashMap<Long, Long> txId2StartOffset = new HashMap<>();
    
    public UndoLogFile(File file) throws FileNotFoundException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
    }

    /**
     * 记录事务开始
     */
    public synchronized void recordTxStart(TransactionID transactionID) {
        // 如果文件没有记录过信息，从头开始
        if (offset == -1) {
            offset = 0;
        }
        txId2StartOffset.put(transactionID.getId(), offset);
    }

    /**
     * 记录原始页
     * 计入事务ID+页原始数据
     *
     * @param transactionID 事务ID
     * @param beforePage    原始页
     */
    public synchronized void recordBeforePageWhenFlushDisk(TransactionID transactionID,
                                                           Page beforePage) {

        try {
            // 事务ID
            raf.writeLong(transactionID.getId());

            // 序列化
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(baos);
            outputStream.writeObject(beforePage);
            byte[] bytes = baos.toByteArray();
            // 写入page占用字节数和page数据
            raf.writeInt(bytes.length);
            raf.write(bytes);

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
            // 事务开始位置
            Long txStartOffset = txId2StartOffset.get(transactionID.getId());

            // 待读取的偏移位置
            raf.seek(raf.length() - offsetAddrInByte);
            long offsetToRead = raf.readLong();

            while (txStartOffset <= offsetToRead) {
                raf.seek(offsetToRead);
                long txId = raf.readLong();
                if (transactionID.getId() == txId) {
                    int pageSize = raf.readInt();
                    byte[] bytes = new byte[pageSize];
                    raf.read(bytes);

                    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                    ObjectInputStream inputStream = new ObjectInputStream(bais);
                    Page beforePage = (Page) inputStream.readObject();

                    DbTable dbTableById = DataBase.getInstance().getDbTableById(beforePage.getPageID().getTableId());
                    dbTableById.getTableFile().writePageToDisk(beforePage);
                    DataBase.getBufferPool().discardPages(Collections.singletonList(beforePage.getPageID()));
                }

                // 已经到开始位置，结束循环
                if (txStartOffset == offsetToRead) {
                    break;
                }

                raf.seek(offsetToRead - offsetAddrInByte);
                offsetToRead = raf.readLong();
            }
            // 复位
            raf.seek(offset);

            txId2StartOffset.remove(transactionID.getId());
        } catch (IOException | ClassNotFoundException e) {
            throw new DbException("undo file rollback failed", e);
        }

    }


}
