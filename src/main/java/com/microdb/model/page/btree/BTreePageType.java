package com.microdb.model.page.btree;

/**
 * b+ tree page类型常量
 *
 * @author zhangjw
 * @version 1.0
 */
public interface BTreePageType {
    /**
     * 存储B+树根节点指针的页
     */
    int ROOT_PTR = 0;
    /**
     * 存储页面的使用状态，链表扩展
     */
    int HEADER = 1;
    /**
     * 存储B+树的内部节点/索引节点
     */
    int INTERNAL = 2;
    /**
     * 存储B+树的页子节点，在聚簇索引树中，页子节点存储的是整行数据
     */
    int LEAF = 3;
}
