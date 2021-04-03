package com.microdb.transaction;

import com.microdb.bufferpool.BufferPool;
import com.microdb.exception.TransactionException;
import com.microdb.model.DataBase;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 锁管理器：记录事务持有哪些页锁、页锁被哪些事务持有
 * 事务的ACID特性需要锁来支持
 * 锁的粒度为页级别
 *
 * @author zhangjw
 * @version 1.0
 */
public class LockManager {


    /**
     * 锁的默认超时时间
     */
    public static final int defaultTimeOutInMillis = 10000;
    /**
     * 管理中的锁
     */
    private final ConcurrentHashMap<PageID, Lock> lockTable;

    /**
     * 管理中的事务
     */
    private final ConcurrentHashMap<TransactionID, List<PageID>> transactionTable;


    public LockManager() {
        lockTable = new ConcurrentHashMap<>();
        transactionTable = new ConcurrentHashMap<>();
    }

    public int getDefaultTimeOutInMillis() {
        return defaultTimeOutInMillis;
    }

    /**
     * 获取页锁
     * 暂时只实现独占锁
     * 线程安全
     * <p>
     * 支持重入(一个事务中某页可能被访问多次)
     */
    public synchronized void acquireLock(Transaction transaction, PageID pageID)
            throws TransactionException {
        System.out.println(String.format("事务开始获取页锁,transaction=%s,pageID=%s", transaction, pageID));
        TransactionID currentTransId = transaction.getTransactionId();

        // 申请x锁
        if (Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {
            // 独占锁实现：
            // 判断该页是否被锁定
            // 如果未被锁定：可成功获取锁，锁定并返回
            // 如果被锁定：判断锁定它的事务表是否包含当前事务，如果包含，锁重入或升级，获取成功；如果不包含，则阻塞。
            while (true) {
                if (!lockTable.containsKey(pageID)) {
                    grantLock(transaction, pageID, currentTransId);
                    return;
                } else {
                    if (lockTable.get(pageID).getLockHolders().contains(currentTransId)) {
                        Lock lock = lockTable.get(pageID);
                        // 锁升级
                        if (Objects.equals(lock.getLockType(), Lock.LockType.SLock)) {
                            lock.setLockType(Lock.LockType.XLock);
                            lockTable.put(pageID, lock);
                        }
                        return;
                    } else {
                        block(System.currentTimeMillis());
                    }
                }
            }
        } else { // 申请s锁
            // 共享锁实现：
            // 判断该页是否被锁定
            // 如果未被锁定：可成功获取锁，锁定并返回
            // 如果被锁定：判断现有锁类型，若是x锁,判断是否由当前事务持有，若是获取成功，否则阻塞；
            //                        若是s锁，获取成功，更新锁的持有者
            while (true) {
                if (!lockTable.containsKey(pageID)) {
                    grantLock(transaction, pageID, currentTransId);
                    return;
                } else {
                    Lock existingLock = lockTable.get(pageID);
                    if (Objects.equals(existingLock.getLockType(), Lock.LockType.XLock)) {
                        if (existingLock.getLockHolders().size() == 1
                                && existingLock.getLockHolders().contains(currentTransId)) {
                            return;
                        } else {
                            block(System.currentTimeMillis());
                        }
                    } else {
                        existingLock.addHolder(currentTransId);
                        return;
                    }
                }
            }
        }
    }

    private void grantLock(Transaction transaction, PageID pageID, TransactionID currentTransId) {
        Lock existingLock = new Lock(pageID, transaction.getLockType());
        existingLock.addHolder(currentTransId);
        lockTable.put(pageID, existingLock);

        List<PageID> pageIDS = transactionTable.getOrDefault(currentTransId, new ArrayList<>());
        pageIDS.add(pageID);
        transactionTable.put(currentTransId, pageIDS);
        return;
    }

    /**
     * 阻塞，使用synchronized 配套的object.wait
     * wait使线程进入等待队列，由其他线程notify唤醒
     * wait带有一个超时时间，用来避免死锁
     *
     * @param startTime 开始时间
     */
    private void block(long startTime) {
        try {
            // 线程进入等待队列睡眠
            wait(LockManager.defaultTimeOutInMillis);

            // 判断是由其他线程唤醒，还是过超时时间唤醒，若超时，事务异常结束
            if (System.currentTimeMillis() - startTime > LockManager.defaultTimeOutInMillis) {
                throw new TransactionException("获取锁超时");
            }
        } catch (InterruptedException e) {
            throw new TransactionException("object.wait() exception");
        }
    }

    /**
     * 释放页锁：释放事务使用的页，并唤醒阻塞中的其他线程
     */
    public synchronized void releaseLock(TransactionID transactionID)
            throws TransactionException {
        System.out.println(String.format("transactionID:%s释放锁", transactionID));

        // 获取事务中使用的页，删除页的锁
        List<PageID> pageIDS = transactionTable.get(transactionID);
        if (pageIDS != null && !pageIDS.isEmpty()) {
            for (PageID pageID : pageIDS) {
                lockTable.remove(pageID);
            }
        }
        // 删除事务
        transactionTable.remove(transactionID);

        // 唤醒睡眠中的线程
        notifyAll();
    }

    public List<PageID> getPageIDs(TransactionID transactionId) {
        return transactionTable.get(transactionId);
    }

    public List<Page> getPages(TransactionID transactionId) {
        List<PageID> pageIDS = transactionTable.get(transactionId);
        BufferPool bufferPool = DataBase.getBufferPool();

        if (pageIDS != null ) {
            return pageIDS.stream()
                    .map(bufferPool::getPage)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }
}
