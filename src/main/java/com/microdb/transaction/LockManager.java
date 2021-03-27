package com.microdb.transaction;

import com.microdb.exception.TransactionException;
import com.microdb.model.page.PageID;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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
    private ConcurrentHashMap<PageID, Lock> lockTable;

    /**
     * 管理中的事务
     */
    private ConcurrentHashMap<TransactionID, List<PageID>> transactionTable;


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
    public synchronized void acquireLock(TransactionID transactionID, PageID pageID)
            throws TransactionException {
        System.out.println(String.format("事务开始获取页锁,transactionID=%s,pageID=%s", transactionID, pageID));

        // 独占锁实现：
        // 判断该页是否被锁定
        // 如果未被锁定：可成功获取锁，锁定并返回
        // 如果被锁定：判断锁定它的事务是否为当前事务，如果是，锁重入，获取成功；如果不是，则阻塞。
        while (true) {
            if (!lockTable.containsKey(pageID)) {
                Lock lock = new Lock(pageID);
                lock.setLockHolder(transactionID);
                lockTable.put(pageID, lock);

                List<PageID> pageIDS = transactionTable.getOrDefault(transactionID, new ArrayList<>());
                pageIDS.add(pageID);
                transactionTable.put(transactionID, pageIDS);

                return;
            } else {
                if (Objects.equals(lockTable.get(pageID).getLockHolder(), transactionID)) {
                    return;
                } else {
                    block(System.currentTimeMillis());
                }
            }
        }
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
        for (PageID pageID : pageIDS) {
            lockTable.remove(pageID);
        }

        // 删除事务
        transactionTable.remove(transactionID);

        // 唤醒睡眠中的线程
        notifyAll();
    }

    public List<PageID> getPageIDs(TransactionID transactionId) {
        return transactionTable.get(transactionId);
    }
}
