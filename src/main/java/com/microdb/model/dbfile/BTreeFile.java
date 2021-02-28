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
        return (int) ((file.length() - BTreeRootPtrPage.rootPtrPageSizeInByte) / Page.defaultPageSizeInByte);
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
        // 查找B+树的根节点Page，如果为不存在，则新增一个leafPage，设置为树的根节点
        BTreeRootPtrPage rootPtrPage = getRootPrtPage();
        // 首次插入数据，初始化rootPage维护第一个leafPage的指针，写入磁盘
        if (rootPtrPage.getRootNodePageID() == null) {
            // 该表尚未插入数据，获取文件中最后一个LeafPage，也是第一个LeafPage，因为上面代码中写入了的leafPage的空数据空间
            BTreePageID firstLeafPageID = new BTreePageID(tableId, getExistPageCount(), BTreePageID.TYPE_LEAF);

            rootPtrPage.setRootPageID(firstLeafPageID);
            // rootPtr更新后刷盘，TODO rootPtr header需要设置
            writePageToDisk(rootPtrPage);
        }

        // 从b+tree中查找按索引查找数据期望插入的leafPage。如果leafPage已满，触发页分裂
        BTreeLeafPage leafPage = this.findLeafPageToPlaceRow(rootPtrPage.getRootNodePageID(), row.getField(keyFieldIndex));
        if (!leafPage.hasEmptySlot()) {
            leafPage = this.splitLeafPage(leafPage, row.getField(keyFieldIndex));
        }

        leafPage.insertRow(row);
        writePageToDisk(leafPage);
    }

    /**
     * 获取rootPtr
     */
    private BTreeRootPtrPage getRootPrtPage() throws IOException {
        // 首次在该页插入数据时，写入空数据
        synchronized (this) {
            if (file.length() == 0) {
                BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bos.write(emptyRootPtrData);
                bos.write(emptyLeafData);
                bos.close();
            }
        }
        // 获取单例的rootPage,如果不存在则新增
        BTreePageID rootPtrPageID = new BTreePageID(this.tableId, 0, BTreePageID.TYPE_ROOT_PTR);

        // 从磁盘中读取rootPage
        return (BTreeRootPtrPage) this.readPageFromDisk(rootPtrPageID);
    }

    /**
     * 分裂页面，返回一个可用页
     * leafPageNeedSplit已满，field没有空间放置，将leafPageNeedSplit分裂
     * 将递归分裂的页面直接刷盘。
     * TODO 存在问题，在并发下是不安全的,后续优化
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
        BTreeLeafPage newRightSibPage = (BTreeLeafPage) getEmptyPage(BTreePageID.TYPE_LEAF);

        Iterator<Row> it = leafPageNeedSplit.getReverseIterator();
        // 原page中的一半数据移动到新页中
        Row[] rowToMove = new Row[(leafPageNeedSplit.getExistRowCount() + 1) / 2];
        int moveCnt = rowToMove.length - 1;
        while (moveCnt >= 0 && it.hasNext()) {
            rowToMove[moveCnt--] = it.next();
        }
        for (int i = rowToMove.length; i > 0; --i) {
            leafPageNeedSplit.deleteRow(rowToMove[i - 1]);
            newRightSibPage.insertRow(rowToMove[i - 1]);
        }

        // 新页的首元素作为键，获取midKey应该插入的父节点page，可能触发父节点分裂，甚至递归分裂
        Field midKey = rowToMove[0].getField(keyFieldIndex);
        // TODO findParentPageToPlaceEntry
        BTreeInternalPage parentInternalPage = findParentPageToPlaceEntry(leafPageNeedSplit.getParentPageID(), midKey);
        BTreePageID oldRightSibPageID = leafPageNeedSplit.getRightSibPageID();

        // 更新新页的右兄弟、做兄弟，更新原页的右兄弟
        newRightSibPage.setRightSibPageID(oldRightSibPageID);
        newRightSibPage.setLeftSibPageID(leafPageNeedSplit.getPageID());
        leafPageNeedSplit.setRightSibPageID(newRightSibPage.getPageID());

        // 设置新页的父节点，新页和原页的父节点一定是同一个
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
     * 找到一个放置keyToInsert的页,可能触发页分裂
     */
    private BTreeInternalPage findParentPageToPlaceEntry(BTreePageID parentPageID, Field keyToInsert) throws IOException {
        BTreeInternalPage parentPage;
        // 如果原父节点是rootPtr，则新增一个internal page，并设置为新的rootNode
        if (parentPageID.getPageType() == BTreePageID.TYPE_ROOT_PTR) {
            // 设置新的rootNode
            parentPage = (BTreeInternalPage) getEmptyPage(BTreePageID.TYPE_INTERNAL);
            BTreeRootPtrPage rootPrtPage = getRootPrtPage();
            rootPrtPage.setRootNodePageID(parentPage.getPageID());
        } else {
            parentPage = (BTreeInternalPage) readPageFromDisk(parentPageID);
        }

        // 父节点没有空槽位则分裂
        if (parentPage.hasEmptySlot()) {
            // TODO
            parentPage = splitInternalPage(parentPage, keyToInsert);
        }

        return parentPage;
    }

    /**
     * 分裂 internalPageNeedSplit 并返回可用的页
     *
     * @param internalPageNeedSplit 待分裂的页
     * @param keyToInsert           索引字段
     * @return 返回可用的页
     */
    private BTreeInternalPage splitInternalPage(BTreeInternalPage internalPageNeedSplit, Field keyToInsert) throws IOException {

        // 获取一个可用的页
        BTreeInternalPage newInternalPage = (BTreeInternalPage) getEmptyPage(BTreePageID.TYPE_INTERNAL);

        // TODO getReverseIterator
        Iterator<BTreeEntry> it = internalPageNeedSplit.getReverseIterator();
        BTreeEntry[] entryToMove = new BTreeEntry[(internalPageNeedSplit.getExistCount() + 1) / 2];
        int moveCnt = entryToMove.length - 1;
        BTreeEntry midEntry = null;
        while (moveCnt >= 0 && it.hasNext()) {
            entryToMove[moveCnt--] = it.next();
        }

        // 将原页中右半部分的数据移入到新页中，右半部分的首个元素midEntry升级为上级索引
        for (int i = entryToMove.length - 1; i >= 0; --i) {
            if (i == 0) {
                // TODO
                internalPageNeedSplit.deleteEntryAndRightChild(entryToMove[i]);
                midEntry = entryToMove[0];
            } else {
                internalPageNeedSplit.deleteEntryAndRightChild(entryToMove[i]);
                // TODO
                newInternalPage.insertEntry(entryToMove[i]);
            }
            // 更新被移动节点的右孩子的父指针
            updateParent(newInternalPage.getPageID(), entryToMove[i].getRightChildPageID());
        }

        if (midEntry == null) {
            // 不会发生
            throw new DbException("splitInternalPage error,no midEntry ");
        }

        // 更新midEntry的左右孩子分别为原页和新页
        midEntry.setLeftChildPageID(internalPageNeedSplit.getPageID());
        midEntry.setRightChildPageID(newInternalPage.getPageID());

        // 从上一级找到一个page，容纳升级的节点midEntry，递归触发上级page的递归分裂,调用findParentPageToPlaceEntry
        // 原页和新页都更新父节点为新获取的父节点
        BTreeInternalPage newParentInternalPage = findParentPageToPlaceEntry(internalPageNeedSplit.getPageID(), midEntry.getKey());
        newParentInternalPage.insertEntry(midEntry);
        internalPageNeedSplit.setParentPageID(newParentInternalPage.getPageID());
        newInternalPage.setParentPageID(newParentInternalPage.getPageID());

        // newInternalPage、newInternalPage、newParentInternalPage 刷盘
        writePageToDisk(internalPageNeedSplit);
        writePageToDisk(newInternalPage);
        writePageToDisk(newParentInternalPage);

        // 比较大小，返回keyToInsert应该插入的页
        if (keyToInsert.compare(PredicateEnum.GREATER_THAN, midEntry.getKey())) {
            return newInternalPage;
        } else {
            return internalPageNeedSplit;
        }
    }

    /**
     * 更新父指针
     */
    private void updateParent(BTreePageID newParentPageID, BTreePageID childPageID) throws IOException {
        BTreePage childPage = (BTreePage) readPageFromDisk(childPageID);
        if (!childPage.getParentPageID().equals(newParentPageID)) {
            childPage = (BTreePage) readPageFromDisk(childPageID);
            childPage.setParentPageID(newParentPageID);

            // TODO 缓存+集中刷盘
            writePageToDisk(childPage);
        }
    }

    /**
     * 返回一个空叶页
     */
    private Page getEmptyPage(int pageType) throws IOException {
        int emptyPageNo = getEmptyPageNo();
        this.writeEmptyPageToDisk(emptyPageNo, pageType);
        BTreePageID BTreePageID = new BTreePageID(tableId, emptyPageNo, pageType);
        return this.readPageFromDisk(BTreePageID);
    }

    private void writeEmptyPageToDisk(int pageNo, int pageType) throws IOException {
        // TODO opt
        int pageSizeInByte = Page.defaultPageSizeInByte; // 除 ROOT_PTR之外的页大小
        if (pageType == BTreePageID.TYPE_ROOT_PTR) {
            pageSizeInByte = BTreeRootPtrPage.rootPtrPageSizeInByte;
        }

        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        rf.seek(BTreeRootPtrPage.defaultPageSizeInByte + (pageNo - 1) * pageSizeInByte);
        rf.write(new byte[pageSizeInByte]);
        rf.close();
    }

    /**
     * 返回一个空的pageNo
     * 获取本文件的根page，找到第一个header page id
     */
    private int getEmptyPageNo() throws IOException {
        BTreeRootPtrPage rootPrtPage = getRootPrtPage();
        BTreePageID headerPageID = rootPrtPage.getFirstHeaderPageID();

        int emptyPageNo = 0;
        if (headerPageID != null) {
            // 读取第一个headerPage
            BTreeHeaderPage headerPage = (BTreeHeaderPage) this.readPageFromDisk(headerPageID);
            int headerPageCount = 0;
            // 遍历headerPage，找到一个含有空槽位的page
            while (headerPage != null && !headerPage.hasEmptySlot()) { // header页存在，但没有空槽位
                headerPageID = headerPage.getNextPageID();
                if (headerPageID != null) {
                    headerPage = (BTreeHeaderPage) this.readPageFromDisk(headerPageID);
                    headerPageCount++;
                } else {
                    break; //遍历完现有的headerPage，全都没有空槽位。
                }
            }

            // 如果headerPage != null，一定存在空的slot
            if (headerPage != null) {
                headerPage = (BTreeHeaderPage) this.readPageFromDisk(headerPageID);
                int emptySlot = headerPage.getFirstEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.maxSlotNum + emptySlot;
            }
        }

        // 没有headerPage或现有的HeaderPage均没有空槽位，则新增一个headerPage
        if (headerPageID == null) {
            synchronized (this) {
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
                byte[] emptyData = new byte[Page.defaultPageSizeInByte];
                bw.write(emptyData);
                bw.close();
                emptyPageNo = getExistPageCount();
            }
        }

        return emptyPageNo;
    }

    /**
     * 查找索引field应该放置的页面，不考虑是否已满
     *
     * @param pageID 数据的根节点PageID
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
