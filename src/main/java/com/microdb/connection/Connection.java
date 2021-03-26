package com.microdb.connection;

import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import com.microdb.transaction.TransactionID;

import java.util.HashMap;

/**
 * 用于跟踪执行过程中的脏页
 *
 * @author zhangjw
 * @version 1.0
 */
public class Connection {
    private static String DIRTY_PAGE_KEY = "dp";
    private static String TRANSACTION_ID_KEY = "tid";
    private static ThreadLocal<HashMap<String, Object>> connection = new ThreadLocal<>();

    /**
     * 获取当前的事务ID
     */
    public static TransactionID currentTransaction() {
        HashMap<String, Object> map = getOrInitThreadMap();
        return (TransactionID) map.get(TRANSACTION_ID_KEY);
    }

    /**
     * connection-passing的方式传递事务：事务ID存储在当前线程/连接中，在调用链的任何位置可以获取
     */
    public static void passingTransaction(TransactionID transactionID) {
        HashMap<String, Object> map = getOrInitThreadMap();
        map.put(TRANSACTION_ID_KEY, transactionID);
    }


    public static void clearTransactionID() {

        HashMap<String, Object> map = getOrInitThreadMap();
        map.remove(TRANSACTION_ID_KEY);
    }

    /**
     * 缓存更新表过程中产生的脏页
     */
    @SuppressWarnings("unchecked")
    public static void cacheDirtyPage(Page page) {
        HashMap<String, Object> map = connection.get();
        HashMap<PageID, Page> pages =
                (HashMap<PageID, Page>) map.compute(DIRTY_PAGE_KEY, (k, v) -> v == null ? v = new HashMap<>() : v);
        page.markDirty();
        pages.put(page.getPageID(), page);
    }

    /**
     * 获取线程内存储的脏页
     * // TODO dirtyPages.size 极限值过大说明page size 配置不合理
     */
    @SuppressWarnings("unchecked")
    public static HashMap<PageID, Page> getDirtyPages() {
        HashMap<String, Object> map = getOrInitThreadMap();
        return (HashMap<PageID, Page>) map.get(DIRTY_PAGE_KEY);

        // FIXME 同一页多次出现的原因
        // Set<Integer> collect = pages.stream().map(x -> x.getPageID().getPageNo()).collect(Collectors.toSet());
        // if (pages.size() != collect.size()) {
        //     throw new DbException("error");
        // }

        // return map.get(DIRTY_PAGE_KEY);
    }

    private static HashMap<String, Object> getOrInitThreadMap() {
        HashMap<String, Object> map = connection.get();
        if (map == null) {
            map = new HashMap<>();
            connection.set(map);
            return map;
        }
        return map;
    }

    /**
     * 清除线程内存储的脏页
     */
    public static void clearDirtyPages() {
        HashMap<String, Object> map = connection.get();
        map.put(DIRTY_PAGE_KEY, new HashMap<>());
    }


}
