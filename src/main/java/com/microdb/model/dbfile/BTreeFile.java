package com.microdb.model.dbfile;

import com.microdb.exception.DbException;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import com.microdb.model.page.btree.*;
import com.microdb.operator.ITableFileIterator;
import com.microdb.operator.PredicateEnum;

import java.io.*;
import java.util.Iterator;

/**
 * 基于B+tree的文件组织
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreeFile implements TableFile {

    /**
     * 磁盘文件
     */
    private File file;

    /**
     * 表结构
     */
    private TableDesc tableDesc;

    /**
     * 表ID,由file的绝对路径生成的唯一ID
     */
    private int tableId;

    /**
     * 索引字段下标，在{@link TableDesc.Attribute}的index
     */
    private int keyFieldIndex;

    public BTreeFile(File file, TableDesc tableDesc, int keyFieldIndex) {
        this.file = file;
        this.tableDesc = tableDesc;
        this.tableId = file.getAbsoluteFile().hashCode();
        this.keyFieldIndex = keyFieldIndex;
    }

    @Override
    public TableDesc getTableDesc() {
        return null;
    }

    @Override
    public Page readPageFromDisk(PageID pageID) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void writePageToDisk(Page page) {
        try {
            byte[] pageData = page.serialize();
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            BTreePageID pageID = (BTreePageID) page.getPageID();
            if (pageID.getPageType() != BTreePageID.TYPE_ROOT_PTR) {
                rf.seek(BTreeRootPtrPage.rootPtrPageSizeInByte + (page.getPageID().getPageNo() - 1) * Page.defaultPageSizeInByte);
            }
            rf.write(pageData);
            rf.close();
        } catch (IOException e) {
            throw new DbException("writePageToDisk failed", e);
        }
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getExistPageCount() {
        throw new UnsupportedOperationException("todo");
    }

    /**
     * 新增一行，如果当前page已满，需要分裂page
     * 分裂page过程中，生成新的page，需要更新新旧page的兄弟指针、父指针、可能触发树上层的内部节点页面分裂甚至递归分裂，需要更新各指针
     *
     * @param row 新的行
     * @throws IOException e
     */
    @Override
    public void insertRow(Row row) throws IOException {
        // 首次在该页插入数据时，写入空数据
        if (file.length() == 0) {
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
            byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
            byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
            bos.write(emptyRootPtrData);
            bos.write(emptyLeafData);
            bos.close();
        }

        // 获取单例的rootPage,如果不存在则新增
        BTreePageID rootPageID = new BTreePageID(this.tableId, 0, BTreePageID.TYPE_ROOT_PTR);

        // 从磁盘中读取rootPage
        BTreeRootPtrPage rootPtrPage = (BTreeRootPtrPage) this.readPageFromDisk(rootPageID);

        // 从rootPage中解析根节点所在页的pageID
        int rootNodePageNo = rootPtrPage.getRootNodePageNo();
        BTreePageID rootNodePageID = null;
        if (rootNodePageNo != 0) { // 表示该表尚未插入数据
            rootNodePageID = new BTreePageID(tableId, rootNodePageNo, rootPtrPage.getRootNodePageType());
        }

        // 首次插入数据，初始化rootPage维护第一个leafPage的指针，写入磁盘
        if (rootNodePageID == null) {
            // 该表尚未插入数据，获取文件中最后一个LeafPage，也是第一个LeafPage，因为上面代码中写入了的leafPage的空数据空间
            BTreePageID firstLeafPageID = new BTreePageID(tableId, getExistPageCount(), BTreePageID.TYPE_LEAF);
            rootPtrPage.setRootPageID(firstLeafPageID);
            writePageToDisk(rootPtrPage);
        }

        // 从b+tree中查找按索引查找数据期望插入的leafPage。如果leafPage已满，触发页分裂
        BTreeLeafPage leafPage = this.findLeafPageToPlaceRow(rootPageID, row.getField(keyFieldIndex));
        if (!leafPage.hasEmptySlot()) {
            leafPage = this.splitLeafPage(leafPage, row.getField(keyFieldIndex));
        }

        leafPage.insertRow(row);
        writePageToDisk(leafPage);
    }

    /**
     * 分裂页面，返回一个可用页
     * leafPageNeedSplit已满，field没有空间放置，将leafPageNeedSplit分裂
     * 将递归分裂的页面直接刷盘。
     * TODO 存在问题，在并发下是不安全的,后续优化，
     * <p>
     * 在 leafPageNeedSplit 的右侧添加一个新的页,并将 leafPageNeedSplit 中一版元素转移到新的页中。
     * 这样导致父节点维护的子节点数量+1，可能触发父节点的页分裂，因此需要递归分裂父节点。
     * 维护同一级的链表,左右兄弟的指针需要更新
     *
     * @param leafPageNeedSplit 待分裂的页
     * @param fieldToInsert     字段值
     * @return 返回一个field可以放置的页
     */
    private BTreeLeafPage splitLeafPage(BTreeLeafPage leafPageNeedSplit, Field fieldToInsert) throws IOException {

        // 获取一个空页作为新的右兄弟
        // TODO
        BTreeLeafPage newRightSibPage = (BTreeLeafPage) getEmptyLeafPage(BTreePageID.TYPE_LEAF);

        // TODO 迭代器
        Iterator<Row> it = leafPageNeedSplit.getReverseIterator();
        // 原page中的一半数据移动到新页中
        Row[] rowToMove = new Row[(leafPageNeedSplit.getExistRowCount() + 1) / 2];
        int moveCnt = rowToMove.length;
        while (moveCnt > 0 && it.hasNext()) {
            rowToMove[moveCnt--] = it.next();
        }
        for (int i = rowToMove.length; i > 0; --i) {
            leafPageNeedSplit.deleteRow(rowToMove[i - 1]);
            newRightSibPage.insertRow(rowToMove[i - 1]);
        }

        // 新页的首元素作为键，获取midKey应该插入的父节点page，可能触发父节点分裂，甚至递归分裂
        Field midKey = rowToMove[0].getField(keyFieldIndex);
        // TODO// TODO set get 左右兄弟逻辑
        BTreeInternalPage parentInternalPage = findParentPageToPlaceEntry(leafPageNeedSplit.getParentPageID(), midKey);
        BTreePageID oldRightSibPageID = leafPageNeedSplit.getRightSibPageID();

        // 更新新页的右兄弟、做兄弟，更新原页的右兄弟
        newRightSibPage.setRightSibPageId(oldRightSibPageID);
        newRightSibPage.setLeftSibPageId(leafPageNeedSplit.getPageID());
        leafPageNeedSplit.setRightSibPageId(newRightSibPage.getPageID());

        // 设置新页的父节点，新页和原页的父节点一定是同一个
        // TODO
        newRightSibPage.setParentPageID(parentInternalPage.getPageID());
        leafPageNeedSplit.setParentPageID(parentInternalPage.getPageID());

        // 父节点Page，插入midKey元素
        BTreeEntry newParentEntry = new BTreeEntry(midKey, leafPageNeedSplit.getPageID(), newRightSibPage.getPageID());
        // TODO
        parentInternalPage.insertEntry(newParentEntry);

        // 刷盘
        writePageToDisk(parentInternalPage);
        writePageToDisk(newRightSibPage);
        writePageToDisk(leafPageNeedSplit);

        // 返回fieldToInsert应该插入的页面
        if (fieldToInsert.compare(PredicateEnum.GREATER_THAN, midKey)) {
            return newRightSibPage;
        } else {
            return leafPageNeedSplit;
        }
    }

    /**
     * 找到一个放置midKey的页
     * 可能触发页分裂
     * @param internalPageID
     * @param midKey
     * @return
     */
    private BTreeInternalPage findParentPageToPlaceEntry(BTreePageID internalPageID, Field midKey) {
        return null;
    }

    private Page getEmptyLeafPage(int typeLeaf) {
        return (Page) new Object();
    }

    /**
     * 查找索引field应该放置的页面，不考虑是否已满
     *
     * @param pageID
     * @param field  索引字段值，在内部节点的查找过程中使用
     * @return 查找field应该放置的页面
     */
    private BTreeLeafPage findLeafPageToPlaceRow(BTreePageID pageID, Field field) throws IOException {

        // 查找到树的最后一层--leafPage
        if (pageID.getPageType() == BTreePageID.TYPE_LEAF) {
            return (BTreeLeafPage) this.readPageFromDisk(pageID);
        }

        // 查找到树的内部节点，在这里递归直至查找到leafPage
        BTreeInternalPage aInternalPage = (BTreeInternalPage) this.readPageFromDisk(pageID);
        BTreeEntry entry;

        Iterator<BTreeEntry> iterator = aInternalPage.getIterator();
        entry = iterator.next();

        BTreePageID nextSearchPageId;
        if (field == null) { // 特殊情况，对于null值，一律从左树中查找
            nextSearchPageId = entry.getLeftChildPageID();
        } else {
            // 如果查找的值>节点值，判断树中下一个节点
            while (field.compare(PredicateEnum.GREATER_THAN, entry.getKey()) && iterator.hasNext()) {
                entry = iterator.next();
            }

            // 如果查找的值<=节点值，进入树的左子树
            if (field.compare(PredicateEnum.LESS_THAN_OR_EQ, entry.getKey())) {
                nextSearchPageId = entry.getLeftChildPageID();
            } else {
                // 如果查找的值>节点值，进入树的右子树
                nextSearchPageId = entry.getRightChildPageID();
            }
        }
        return this.findLeafPageToPlaceRow(nextSearchPageId, field);
    }

    @Override
    public ITableFileIterator getIterator() {
        return null;
    }
}
