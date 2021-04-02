package com.microdb.logging;

import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.page.Page;
import com.microdb.transaction.TransactionID;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 重做日志，用于数据库运行时崩溃后的数据恢复
 *
 * @author zhangjw
 * @version 1.0
 */
public class RedoLogFile {
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

    public RedoLogFile(File file) throws FileNotFoundException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
    }

    /**
     * 记录事务开始
     */
    public synchronized void recordTxStart(TransactionID transactionID) throws IOException {
        // 如果文件没有记录过信息，从头开始
        if (offset == -1) {
            offset = 0;
        }

        raf.seek(offset);
        raf.writeInt(LogRecordType.TX_START);
        raf.writeLong(transactionID.getId());
        // 在本次记录的末尾处写入记录的开始位置
        raf.writeLong(offset);

        // 更新写指针
        offset = raf.getFilePointer();

        raf.getChannel().force(true);
    }

    /**
     * 记录原始页
     * 计入事务ID+页原始数据
     *
     * @param transactionID 事务ID
     * @param beforePage    原始页
     */
    public synchronized void recordCommittedPage(TransactionID transactionID,
                                                 Page beforePage,
                                                 Page afterPage) throws IOException {

        raf.writeInt(LogRecordType.PAGE_FLUSH);
        // 事务ID
        raf.writeLong(transactionID.getId());
        // 页数据
        writePage(beforePage);
        writePage(afterPage);
        raf.writeLong(offset);
        // 更新offset
        offset = raf.getFilePointer();
    }

    /**
     * 记录事务提交
     */
    public synchronized void recordTxCommit(TransactionID transactionID) throws IOException {
        raf.writeInt(LogRecordType.TX_COMMIT);
        raf.writeLong(transactionID.getId());
        // 在本次记录的末尾处写入记录的开始位置
        raf.writeLong(offset);
        // 更新写指针
        offset = raf.getFilePointer();
    }

    /**
     * 恢复已提交的事务为修改后的数据，恢复未提交的事务为修改前的数据，
     */
    public synchronized void recover() throws IOException {
        // 从后向前遍历文件，找出未提交事务和已提交的事务
        offset = raf.length();
        // 文件中所有记录的起始地址
        ArrayList<Long> recordStartPosLis = new ArrayList<>();
        raf.seek(offset - offsetAddrInByte);
        long recordStartPos = raf.readLong();
        recordStartPosLis.add(recordStartPos);
        while (recordStartPos >= 0) {
            if (recordStartPos == 0) {
                recordStartPosLis.add(recordStartPos);
                break;
            }
            raf.seek(recordStartPos - offsetAddrInByte);
            recordStartPos = raf.readLong();
            recordStartPosLis.add(recordStartPos);
        }

        // HashMap<Long, Long> allTxs2Pos = new HashMap<>();
        HashMap<Long, Long> commitTxs2Pos = new HashMap<>();
        for (Long pos : recordStartPosLis) {
            raf.seek(pos);
            int recordType = raf.readInt();
            if (recordType == LogRecordType.TX_START) {
                // allTxs2Pos.put(raf.readLong(), pos);
                txId2StartOffset.put(raf.readLong(), pos);
            }
            if (recordType == LogRecordType.TX_COMMIT) {
                commitTxs2Pos.put(raf.readLong(), pos);
            }
        }

        for (Map.Entry<Long, Long> entry : txId2StartOffset.entrySet()) {
            if (commitTxs2Pos.containsKey(entry.getKey())) {
                // 事务已提交，刷盘
                recoverCommittedPage(entry.getKey());
            } else {
                // 事务未提交，回滚
                rollback(entry.getKey());
            }
        }
        txId2StartOffset.clear();
    }

    /**
     * 将事务修改过的页面，在磁盘刷回原始版本，缓存中丢弃
     *
     * @param txIdRollback 回滚的事务
     */
    public synchronized void rollback(long txIdRollback) throws IOException {

        // 事务开始位置
        Long txStartOffset = txId2StartOffset.get(txIdRollback);
        // 待读取的偏移位置
        raf.seek(raf.length() - offsetAddrInByte);
        long offsetToRead = raf.readLong();

        while (txStartOffset <= offsetToRead) {
            raf.seek(offsetToRead + 4);// 跳过log类型
            long txId = raf.readLong();
            if (txIdRollback == txId) {
                Page beforePage = this.readPage();
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
    }

    private Page readPage() throws IOException {
        Page beforePage;
        int pageSize = raf.readInt();
        byte[] bytes = new byte[pageSize];
        raf.read(bytes);

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream inputStream = new ObjectInputStream(bais);
        try {
            beforePage = (Page) inputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new DbException("deserialize error", e);
        }
        return beforePage;
    }

    private void writePage(Page beforePage) throws IOException {
        // 序列化
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(baos);
        outputStream.writeObject(beforePage);
        byte[] bytes = baos.toByteArray();
        // 写入page占用字节数和page数据
        raf.writeInt(bytes.length);
        raf.write(bytes);
    }

    private void recoverCommittedPage(Long txIdCommit) throws IOException {
        // 事务开始位置
        Long txStartOffset = txId2StartOffset.get(txIdCommit);
        // 待读取的偏移位置
        raf.seek(raf.length() - offsetAddrInByte);
        long offsetToRead = raf.readLong();

        while (txStartOffset <= offsetToRead) {
            raf.seek(offsetToRead);// 跳过log类型
            int recordType = raf.readInt();
            if (recordType == LogRecordType.PAGE_FLUSH) {
                raf.seek(offsetToRead + 4);// 跳过log类型
                long txId = raf.readLong();
                if (txIdCommit == txId) {
                    Page beforePage = this.readPage();
                    Page afterPage = this.readPage();
                    DbTable dbTableById = DataBase.getInstance().getDbTableById(beforePage.getPageID().getTableId());
                    dbTableById.getTableFile().writePageToDisk(afterPage);
                    DataBase.getBufferPool().discardPages(Collections.singletonList(beforePage.getPageID()));
                }
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
    }

    /**
     * 刷盘
     */
    public synchronized void flush() throws IOException {
        raf.getChannel().force(true);

    }
}
