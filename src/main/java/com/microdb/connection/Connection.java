package com.microdb.connection;

import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;

import java.util.HashMap;

/**
 * 用于跟踪执行过程中的脏页
 *
 * @author zhangjw
 * @version 1.0
 */
public class Connection {
    private static ThreadLocal<HashMap<String, HashMap<PageID, Page>>> connection = new ThreadLocal<>();

    private static String DIRTY_PAGE_KEY = "dp";

    /**
     * 缓存更新表过程中产生的脏页
     */
    public static void cacheDirtyPage(Page page) {
        HashMap<String, HashMap<PageID, Page>> map = connection.get();
        if (map == null) {
            map = new HashMap<>();
            connection.set(map);
        }
        HashMap<PageID, Page> pages = map.compute(DIRTY_PAGE_KEY, (k, v) -> v == null ? v = new HashMap<>() : v);
        page.markDirty();
        pages.put(page.getPageID(), page);
    }

    /**
     * 获取线程内存储的脏页
     * // TODO dirtyPages.size 极限值过大说明page size 配置不合理
     */
    public static HashMap<PageID, Page> getDirtyPages() {
        HashMap<String, HashMap<PageID, Page>> map = connection.get();
        if (map == null) {
            map = new HashMap<>();
            map.put(DIRTY_PAGE_KEY, new HashMap<>());
            connection.set(map);
        }
        return map.get(DIRTY_PAGE_KEY);

        // FIXME 同一页多次出现的原因
        // Set<Integer> collect = pages.stream().map(x -> x.getPageID().getPageNo()).collect(Collectors.toSet());
        // if (pages.size() != collect.size()) {
        //     throw new DbException("error");
        // }

        // return map.get(DIRTY_PAGE_KEY);
    }

    /**
     * 清除线程内存储的脏页
     */
    public static void clearDirtyPages() {
        HashMap<String, HashMap<PageID, Page>> map = connection.get();
        // connection.set(null);
        map.put(DIRTY_PAGE_KEY, new HashMap<>());
    }

}
