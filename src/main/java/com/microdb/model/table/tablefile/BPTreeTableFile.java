package com.microdb.model.table.tablefile;

import com.microdb.annotation.VisibleForTest;
import com.microdb.connection.Connection;
import com.microdb.exception.DbException;
import com.microdb.model.DataBase;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.field.Field;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import com.microdb.model.page.bptree.*;
import com.microdb.operator.PredicateEnum;
import com.microdb.operator.bptree.IndexPredicate;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 基于B+tree的文件组织
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeTableFile implements TableFile {

    /**
     * 磁盘文件
     */
    private final File file;

    /**
     * 表结构
     */
    private final TableDesc tableDesc;

    /**
     * 表ID,由file的绝对路径生成的唯一ID
     */
    private final int tableId;

    /**
     * 索引字段下标，在{@link TableDesc#attributes}的index
     */
    private final int keyFieldIndex;


    public BPTreeTableFile(File file, TableDesc tableDesc, int keyFieldIndex) {
        this.file = file;
        this.tableDesc = tableDesc;
        this.tableId = file.getAbsoluteFile().hashCode();
        this.keyFieldIndex = keyFieldIndex;
    }

    public int getKeyFieldIndex() {
        return keyFieldIndex;
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
        BPTreeRootPtrPage rootPtrPage = getRootPrtPage();
        // 首次插入数据，初始化rootPage维护第一个leafPage的指针，写入磁盘
        if (rootPtrPage.getRootNodePageID() == null) {
            // 该表尚未插入数据，获取文件中最后一个LeafPage，也是第一个LeafPage，因为上面代码中写入了的leafPage的空数据空间
            BPTreePageID firstLeafPageID = new BPTreePageID(tableId, getExistPageCount(), BPTreePageType.LEAF);

            rootPtrPage.setRootPageID(firstLeafPageID);
            // rootPtr更新后刷盘，TODO rootPtr header需要设置
            Connection.cacheDirtyPage(rootPtrPage);
        }

        // 从b+tree中查找按索引查找数据期望插入的leafPage。如果leafPage已满，触发页分裂
        BPTreeLeafPage leafPage = this.findLeafPage(rootPtrPage.getRootNodePageID(), row.getField(keyFieldIndex));
        if (!leafPage.hasEmptySlot()) {
            leafPage = this.splitLeafPage(leafPage, row.getField(keyFieldIndex));
            System.out.println("页分裂" + leafPage.getPageID());
            if (leafPage.getParentPageID().getPageNo() == 0) {
                throw new DbException("叶页分裂后，父parent no 不应为0");
            }
        }

        System.out.println("insertRow:" + row);
        leafPage.insertRow(row);
        try {
            Connection.cacheDirtyPage(leafPage);
            System.out.println("writePageToDisk:" + leafPage.getPageID());
        } catch (Exception e) {
            System.out.println("error row=" + row.toString());
            throw new DbException(e);
        }
    }

    /**
     * 删除一行，当本页没有足够的数据，会触发与兄弟页合并或者与兄弟页一起重分布元素
     */
    @Override
    public void deleteRow(Row row) throws IOException {
        BPTreePageID bpTreePageID = new BPTreePageID(tableId, row.getRowID().getPageID().getPageNo(), BPTreePageType.LEAF);
        BPTreeLeafPage page = (BPTreeLeafPage) DataBase.getBufferPool().getPage(bpTreePageID);
        page.deleteRow(row);
        Connection.cacheDirtyPage(page);

        // 数量不足一半时，与兄弟节点重新分布元素
        if (page.isNeedRedistribute()) {
            redistributePage(page);
        }

    }

    /**
     * 重分配页中的元素
     * 通过page的父节点找page的左右兄弟，保证他们在同一个页上、有同一个父节点。
     * 在父节点中找到相关的entry
     *
     * @param pageLessThanHalfFull 不及半满的页
     */
    private void redistributePage(BPTreePage pageLessThanHalfFull) throws IOException {
        // 获取待分配页的父节点
        BPTreePageID parentPageID = pageLessThanHalfFull.getParentPageID();
        BPTreeInternalPage parentPage = null;
        BPTreeEntry leftParentEntry = null;
        BPTreeEntry rightParentEntry = null;

        // 判断父节点的类型，如果是internal页，获取指向目标页的两个entry
        if (parentPageID.getPageType() == BPTreePageType.INTERNAL) {
            parentPage = (BPTreeInternalPage) DataBase.getBufferPool().getPage(parentPageID);
            Iterator<BPTreeEntry> iterator = parentPage.getIterator();
            while (iterator.hasNext()) {
                BPTreeEntry entry = iterator.next();
                if (entry.getLeftChildPageID().equals(pageLessThanHalfFull.getPageID())) {
                    rightParentEntry = entry;
                    break;
                } else if (entry.getRightChildPageID().equals(pageLessThanHalfFull.getPageID())) {
                    leftParentEntry = entry;
                }
            }
        } else {
            // parentPageID.getPageType() = rootPtr
            // ignore
        }

        if (pageLessThanHalfFull.getPageID().getPageType() == BPTreePageType.LEAF) {
            redistributeLeafPage((BPTreeLeafPage) pageLessThanHalfFull, parentPage, leftParentEntry, rightParentEntry);
        } else {
            redistributeInternalPage((BPTreeInternalPage) pageLessThanHalfFull, parentPage, leftParentEntry, rightParentEntry);
        }
    }

    /**
     * 重分布内部节点
     * 评估左右兄弟页现有元素数量，如果均不及半满，则合并两页；如果兄弟页有多页元素，则取来补充
     *
     * @param pageLessThanHalfFull 不及半满的页
     * @param parentPage           pageLessThanHalfFull 的父页
     * @param leftEntry            指向pageLessThanHalfFull的两个entry中左侧entry
     * @param rightEntry           指向pageLessThanHalfFull的两个entry中右侧entry
     */
    private void redistributeInternalPage(BPTreeInternalPage pageLessThanHalfFull,
                                          BPTreeInternalPage parentPage,
                                          BPTreeEntry leftEntry,
                                          BPTreeEntry rightEntry) throws IOException {
        if (leftEntry != null && leftEntry.getLeftChildPageID() != null) {
            BPTreePageID leftSibPageID = leftEntry.getLeftChildPageID();
            BPTreeInternalPage leftSibPage = (BPTreeInternalPage) DataBase.getBufferPool().getPage(leftSibPageID);
            if (leftSibPage.isMeetMergeCount()) {
                mergeInternalPage(leftSibPage, pageLessThanHalfFull, parentPage, leftEntry);
            } else {
                fetchFromLeftInternalPage(pageLessThanHalfFull, leftSibPage, parentPage, leftEntry);
            }
        } else if (rightEntry != null && rightEntry.getRightChildPageID() != null) {
            BPTreePageID rightSibPageID = rightEntry.getRightChildPageID();
            BPTreeInternalPage rightSibPage = (BPTreeInternalPage) DataBase.getBufferPool().getPage(rightSibPageID);
            if (rightSibPage.isMeetMergeCount()) {
                mergeInternalPage(pageLessThanHalfFull, rightSibPage, parentPage, rightEntry);
            } else {
                fetchFromRightInternalPage(pageLessThanHalfFull, rightSibPage, parentPage, rightEntry);
            }
        }
    }

    /**
     * 从左页中挪走元素至目标页面，直到两个页面都至少达到半满
     *
     * @param pageLessHalfFull entry应移入的新页
     * @param leftPage         提供entry的页
     * @param parentPage       targetPage和leftPage的父页
     * @param leftEntry        指向targetPage的两个entry中左侧的entry
     */
    private void fetchFromLeftInternalPage(BPTreeInternalPage pageLessHalfFull,
                                           BPTreeInternalPage leftPage,
                                           BPTreeInternalPage parentPage,
                                           BPTreeEntry leftEntry) throws IOException {

        // 计算需要移动的个数，使移动后，两个page里的entry数量均等
        int entryNumToMove = (leftPage.getExistCount() - pageLessHalfFull.getExistCount()) / 2;

        // fetch 余下的元素数量必须>2
        // 如果余下0，1，后续该页删除元素时，无法处理merge或fetch或过程中的子叶关系更新条件
        // 当internal page 的容量过小时，无法满足条件，需要调大 page size
        if (leftPage.getExistCount() - entryNumToMove < 2) {
            throw new DbException("internal page 容量过小,无法处理页合并或元素挪用，请调大page size");
        }

        BPTreeEntry[] bpTreeEntries = new BPTreeEntry[entryNumToMove];
        // 需要移动的元素装入entryToMove，当从左侧page取元素时，从最右端取entryNumToMove个元素
        Iterator<BPTreeEntry> it = leftPage.getReverseIterator();
        int cntIndex = bpTreeEntries.length - 1;

        while (cntIndex >= 0 && it.hasNext()) {
            bpTreeEntries[cntIndex--] = it.next();
        }

        for (int i = bpTreeEntries.length - 1; i >= 0; --i) {
            BPTreeEntry entryToMove = bpTreeEntries[i];
            // 删除移动entry、设置其右孩子的新父页
            leftPage.deleteEntryFromTheRight(entryToMove);
            updateParent(pageLessHalfFull.getPageID(), entryToMove.getRightChildPageID());

            // 旋转(entryToMove围绕leftEntry右旋，entryToMove是leftEntry的右子节点，右旋后，entryToMove变为父节点，leftEntry变为其右子节点)
            // 较难推导，最好画图
            BPTreePageID pageID = entryToMove.getRightChildPageID();
            // leftEntry 是发生转移的两个page的父entry，转移过程中，leftEntry需要下降到子级
            entryToMove.setLeftChildPageID(leftEntry.getLeftChildPageID());
            entryToMove.setRightChildPageID(leftEntry.getRightChildPageID());
            entryToMove.setRowID(leftEntry.getRoeID());
            parentPage.updateEntry(entryToMove); //更新后，新的父entry生成，key是当前遍历的entry的key

            leftEntry.setLeftChildPageID(pageID);
            // 下降到右子树的父元素，需要设置新的右子page，应为pageLessHalfFull的第一个entry的左子page
            leftEntry.setRightChildPageID(pageLessHalfFull.getIterator().next().getLeftChildPageID());

            pageLessHalfFull.insertEntry(leftEntry);
            leftEntry = entryToMove;
        }
        Connection.cacheDirtyPage(pageLessHalfFull);
        Connection.cacheDirtyPage(leftPage);
        Connection.cacheDirtyPage(parentPage);
    }

    /**
     * 从右页面中挪走元素至目标页面，直到两个页面至少半满，并更新右侧页面在父页中的rightEntry
     *
     * @param pageLessHalfFull entry应移入的新页
     * @param rightPage        提供entry的页
     * @param parentPage       targetPage和rightPage的父页
     * @param rightEntry       指向targetPage的两个entry中右侧的entry
     */
    private void fetchFromRightInternalPage(BPTreeInternalPage pageLessHalfFull,
                                            BPTreeInternalPage rightPage,
                                            BPTreeInternalPage parentPage,
                                            BPTreeEntry rightEntry) throws IOException {
        // 计算需要移动的个数，使移动后，
        // 两个page里的entry数量均等
        int entryNumToMove = (rightPage.getExistCount() - pageLessHalfFull.getExistCount()) / 2;

        // fetch 余下的元素数量必须>2
        // 如果余下0，1，后续该页删除元素时，无法处理merge或fetch或过程中的子叶关系更新条件
        // 当internal page 的容量过小时，无法满足条件，需要调大 page size
        if (rightPage.getExistCount() - entryNumToMove < 2) {
            throw new DbException("internal page 容量过小,无法处理页合并或元素挪用，请调大page size");
        }

        BPTreeEntry[] entriesToMove = new BPTreeEntry[entryNumToMove];
        Iterator<BPTreeEntry> iterator = rightPage.getIterator();
        int cntIndex = 0;
        while (cntIndex < entriesToMove.length && iterator.hasNext()) {
            entriesToMove[cntIndex++] = iterator.next();
        }

        for (BPTreeEntry entryToMove : entriesToMove) {
            rightPage.deleteEntryFromTheLeft(entryToMove);
            BPTreePageID leftChildPageIDOfEntryToMove = entryToMove.getLeftChildPageID();

            // 被移动的entry所指向的子页面需要设置新的父页
            updateParent(pageLessHalfFull.getPageID(), entryToMove.getLeftChildPageID());

            // 旋转(entryToMove围绕rightEntry左旋，entryToMove是rightEntry的右子节点，左旋后，entryToMove变为父节点，rightEntry变为其左子节点)
            // 较难推导，最好画图
            // from页中entry被推向父页
            entryToMove.setLeftChildPageID(rightEntry.getLeftChildPageID());
            entryToMove.setRightChildPageID(rightEntry.getRightChildPageID());
            entryToMove.setRowID(rightEntry.getRoeID());
            parentPage.updateEntry(entryToMove);
            // 父页中的插入左页
            rightEntry.setRightChildPageID(leftChildPageIDOfEntryToMove);
            rightEntry.setLeftChildPageID(pageLessHalfFull.getReverseIterator().next().getRightChildPageID());
            pageLessHalfFull.insertEntry(rightEntry);

            rightEntry = entryToMove;

        }

        Connection.cacheDirtyPage(pageLessHalfFull);
        Connection.cacheDirtyPage(rightPage);
        Connection.cacheDirtyPage(parentPage);
    }

    /**
     * leftPage 与 rightPage合并：
     * rightPage元素挪到leftPage中
     * 在父页中指向rightPage的entry要拉下来，删除其子指针，放入leftPage
     * 由于父页中少了一个entry，可能触发递归合并
     * 更新父指针
     * 将rightPage标记为'未使用'
     *
     * @param leftPage   左页
     * @param rightPage  右页
     * @param parentPage 父页
     * @param entry      父页中指向左页和右页的entry
     */
    private void mergeInternalPage(BPTreeInternalPage leftPage,
                                   BPTreeInternalPage rightPage,
                                   BPTreeInternalPage parentPage,
                                   BPTreeEntry entry) throws IOException {
        // 两页合并，父页中指向两个子页的的entry应删除
        deleteParentEntry(leftPage, parentPage, entry);
        Connection.cacheDirtyPage(parentPage);

        // 将原来在父页的entry拉下来到下一级
        entry.setLeftChildPageID(leftPage.getReverseIterator().next().getRightChildPageID());
        entry.setRightChildPageID(rightPage.getIterator().next().getLeftChildPageID());
        leftPage.insertEntry(entry);

        // 右侧页的数据挪到左侧页中
        Iterator<BPTreeEntry> iterator = rightPage.getIterator();
        while (iterator.hasNext()) {
            BPTreeEntry entryInRightPage = iterator.next();
            rightPage.deleteEntryFromTheLeft(entryInRightPage);
            // 右页删除后，原来右页的子页的父节点要更新为leftPage
            updateParent(leftPage.getPageID(), entryInRightPage.getLeftChildPageID());
            updateParent(leftPage.getPageID(), entryInRightPage.getRightChildPageID());
            leftPage.insertEntry(entryInRightPage);
        }
        Connection.cacheDirtyPage(leftPage);

        // 右侧页标记为未使用
        markPageUnused(rightPage.getPageID().getPageNo());
    }

    /**
     * 首先判断兄弟页面是否有多余的节点，如果有，则与兄弟页面平分。
     * 如果兄弟页面中元素数量也少于1/2，则与兄弟页面合并，并更新父、子、兄弟页面索引
     *
     * @param leafPageLessThanHalfFull 元素数量不足最大荷载一半的叶子页面
     * @param parentPage
     * @param leftParentEntry          指向 leafPageLessThanHalfFull 的两个entry中左侧entry
     * @param rightParentEntry         指向 leafPageLessThanHalfFull 的两个entry中左侧entry
     */
    private void redistributeLeafPage(BPTreeLeafPage leafPageLessThanHalfFull,
                                      BPTreeInternalPage parentPage,
                                      BPTreeEntry leftParentEntry,
                                      BPTreeEntry rightParentEntry) throws IOException {
        // 找到同一个父节点的左侧page，判断他容量是否不足一半，则合并，否则stealFromLeafPage，右侧page同理

        if (leftParentEntry != null && leftParentEntry.getLeftChildPageID() != null) {
            // 与left兄弟页分配
            BPTreePageID leftChildPageID = leftParentEntry.getLeftChildPageID();
            BPTreeLeafPage leftLeafPage = (BPTreeLeafPage) DataBase.getBufferPool().getPage(leftChildPageID);
            if (leftLeafPage.isMeetMergeCount()) {
                mergeLeafPage(leftLeafPage, leafPageLessThanHalfFull, parentPage, leftParentEntry);
            } else {
                fetchFromLeftLeafPage(leafPageLessThanHalfFull, leftLeafPage, parentPage, leftParentEntry);
            }
        } else if (rightParentEntry != null && rightParentEntry.getRightChildPageID() != null) {
            // 与right兄弟页分配
            BPTreePageID rightChildPageID = rightParentEntry.getRightChildPageID();
            BPTreeLeafPage rightLeafPage = (BPTreeLeafPage) DataBase.getBufferPool().getPage(rightChildPageID);
            if (rightLeafPage.isMeetMergeCount()) {
                mergeLeafPage(leafPageLessThanHalfFull, rightLeafPage, parentPage, rightParentEntry);
            } else {
                fetchFromRightLeafPage(leafPageLessThanHalfFull, rightLeafPage, parentPage, rightParentEntry);
            }
        }
    }

    /**
     * 从左侧页面中挪走元素至目标页面，直到两个页面至少半满，并更新右侧页面在父页中的entry
     *
     * @param targetPage      目标页
     * @param leftLeafPage    leftLeafPage
     * @param parentPage      父页
     * @param leftParentEntry 左页页面entry
     */
    private void fetchFromLeftLeafPage(BPTreeLeafPage targetPage,
                                       BPTreeLeafPage leftLeafPage,
                                       BPTreeInternalPage parentPage,
                                       BPTreeEntry leftParentEntry) {

        int rowNumToMove = (leftLeafPage.getExistRowCount() - targetPage.getExistRowCount()) / 2;
        Row[] rowsToMove = new Row[rowNumToMove];
        int rowToMoveIndex = rowsToMove.length - 1;
        Iterator<Row> rowIterator = leftLeafPage.getReverseIterator();

        while (rowToMoveIndex >= 0 & rowIterator.hasNext()) {
            rowsToMove[rowToMoveIndex--] = rowIterator.next();
        }

        for (Row row : rowsToMove) {
            leftLeafPage.deleteRow(row);
            targetPage.insertRow(row);
        }


        leftParentEntry.setKey(targetPage.getRowIterator().next().getField(keyFieldIndex));
        parentPage.updateEntry(leftParentEntry);

        Connection.cacheDirtyPage(targetPage);
        Connection.cacheDirtyPage(leftLeafPage);
        Connection.cacheDirtyPage(parentPage);
    }


    /**
     * 从右侧页面中挪走元素至目标页面，直到两个页面至少半满，并更新右侧页面在父页中的entry
     *
     * @param targetPage       元素转移的的目标页
     * @param rightLeafPage    右侧页面
     * @param parentPage       父页
     * @param rightParentEntry 右侧页面entry
     */
    private void fetchFromRightLeafPage(BPTreeLeafPage targetPage,
                                        BPTreeLeafPage rightLeafPage,
                                        BPTreeInternalPage parentPage,
                                        BPTreeEntry rightParentEntry) {

        int rowNumToMove = (rightLeafPage.getExistRowCount() - targetPage.getExistRowCount()) / 2;
        Row[] rowsToMove = new Row[rowNumToMove];
        int rowToMoveIndex = rowsToMove.length - 1;
        Iterator<Row> rowIterator = rightLeafPage.getRowIterator();
        while (rowToMoveIndex >= 0 & rowIterator.hasNext()) {
            rowsToMove[rowToMoveIndex--] = rowIterator.next();
        }

        for (Row row : rowsToMove) {
            rightLeafPage.deleteRow(row);
            targetPage.insertRow(row);
        }


        if (rowNumToMove > 0) {
            rightParentEntry.setKey(rightLeafPage.getRowIterator().next().getField(keyFieldIndex));
            parentPage.updateEntry(rightParentEntry);
        }

        Connection.cacheDirtyPage(targetPage);
        Connection.cacheDirtyPage(rightLeafPage);
        Connection.cacheDirtyPage(parentPage);
    }

    /**
     * 将右侧的page中的row移动到左侧中
     * 删除父节点中相应的entry
     * 更新左右兄弟指针
     * 更改删掉的右page的slot标识，用于页重用
     * 删掉已删除的页在父页中的entry
     *
     * @param leftPage    两个待合并页中的左侧页面
     * @param rightPage   两个待合并页中的右侧页面
     * @param parentPage  两个待合并页的父页
     * @param parentEntry 指向待合并页的entry
     */
    private void mergeLeafPage(BPTreeLeafPage leftPage,
                               BPTreeLeafPage rightPage,
                               BPTreeInternalPage parentPage,
                               BPTreeEntry parentEntry) throws IOException {

        // right page 中所有行移入 left Page
        Row[] rowToDelete = new Row[rightPage.getExistRowCount()];
        int deleteIndex = rowToDelete.length - 1;
        Iterator<Row> rowIterator = rightPage.getRowIterator();
        while (deleteIndex >= 0 && rowIterator.hasNext()) {
            rowToDelete[deleteIndex--] = rowIterator.next();
        }
        for (Row row : rowToDelete) {
            rightPage.deleteRow(row);
            leftPage.insertRow(row);
        }

        // 更新左右兄弟指针
        BPTreePageID rightSibPageID = rightPage.getRightSibPageID();
        leftPage.setRightSibPageID(rightSibPageID);
        if (rightSibPageID != null) {
            BPTreeLeafPage rightSibPage = (BPTreeLeafPage) DataBase.getBufferPool().getPage(rightSibPageID);
            rightSibPage.setLeftSibPageID(leftPage.getPageID());
        }
        // 刷盘
        Connection.cacheDirtyPage(rightPage);
        Connection.cacheDirtyPage(leftPage);

        // 将右页标记为删除
        markPageUnused(rightPage.getPageID().getPageNo());
        deleteParentEntry(leftPage, parentPage, parentEntry);
    }


    /**
     * 删除internal页中entry，如果删除后，元素不足容量一半，从兄弟页面中挪来元素补充，或者与兄弟页面合并
     * 如果页中没有元素了，说明只剩这最后一个internal页，删除后，需要将子级的leafPage设置为根
     *
     * @param leftPage      待删除entry所指向的子页（其中的一个）
     * @param parentPage    父页
     * @param entryToDelete 待删除entry
     */
    private void deleteParentEntry(BPTreePage leftPage,
                                   BPTreeInternalPage parentPage,
                                   BPTreeEntry entryToDelete) throws IOException {

        // 由于合并后删除右页，entryToDelete需要删除指向右页的rightChildPageNo指针
        parentPage.deleteEntryAndRightChildPage(entryToDelete);
        Connection.cacheDirtyPage(parentPage);
        // if (parentPage.getParentPageID().getPageType() == BPTreePageType.ROOT_PTR && parentPage.isEmpty()) {
        if (parentPage.isEmpty()) {
            // 当父页变空，说明没有可以合并的页，即不再有其他internal page了，需要将leafPage挂在rootPtr下
            BPTreePageID rootPrtPageID = parentPage.getParentPageID();
            if (rootPrtPageID.getPageType() != BPTreePageType.ROOT_PTR) {
                throw new DbException("try delete none root ptr page");
            }
            BPTreeRootPtrPage rootPtrPage = (BPTreeRootPtrPage) DataBase.getBufferPool().getPage(rootPrtPageID);
            leftPage.setParentPageID(rootPrtPageID);
            rootPtrPage.setRootNodePageID(leftPage.getPageID());

            // 刷盘
            Connection.cacheDirtyPage(leftPage);
            Connection.cacheDirtyPage(rootPtrPage);

            // 父page标记为未使用
            markPageUnused(parentPage.getPageID().getPageNo());
        } else if (parentPage.isNeedRedistribute()) {
            redistributePage(parentPage);
        }
    }

    /**
     * 将pageNo标记为未使用
     */
    private void markPageUnused(int pageNo) throws IOException {
        // TODO
        // // 查找B+tree文件中的的第一个headerPage
        // BPTreeRootPtrPage rootPrtPage = getRootPrtPage();
        // BPTreePageID headerPageID = rootPrtPage.getFirstHeaderPageID();
        // BPTreePageID prevHeaderPageID = null;
        //
        // int headerPageCount = 0;
        // // 如果第一个headerPage尚未创建，则新建
        // if (headerPageID == null) {
        //     BPTreeHeaderPage emptyHeaderPage = (BPTreeHeaderPage) getEmptyPage(BPTreePageType.HEADER);
        //     headerPageID = emptyHeaderPage.getPageID();
        //     rootPrtPage.setFirstHeaderPageID(headerPageID);
        //     emptyHeaderPage.markAllUsed();
        //     // 刷盘
        //     writePageToDisk(rootPrtPage);
        //     writePageToDisk(emptyHeaderPage);
        // } else {
        //     // 尝试获取emptyPageNo槽位所在的headerPage
        //     while (headerPageID != null && (headerPageCount + 1) * BPTreeHeaderPage.maxSlotNum < pageNo) {
        //         BPTreeHeaderPage headerPage = (BPTreeHeaderPage) readPageFromDisk(headerPageID);
        //         prevHeaderPageID = headerPageID;
        //         headerPageID = headerPage.getNextPageID();
        //         headerPageCount++;
        //     }
        //
        //     // 如果上面尝试获取时未找到，说明emptyPageNo槽位所在的headerPage或headerPage链表前面的headerPage尚未创建，
        //     // 应创建相关headerPage，并设置链表的节点索引
        //     while ((headerPageCount + 1) * BPTreeHeaderPage.maxSlotNum < pageNo) {
        //         BPTreeHeaderPage prevPage = (BPTreeHeaderPage) readPageFromDisk(prevHeaderPageID);
        //         BPTreeHeaderPage emptyPageHeaderPage = (BPTreeHeaderPage) getEmptyPage(BPTreePageType.HEADER);
        //         headerPageID = emptyPageHeaderPage.getPageID();
        //         emptyPageHeaderPage.setPrevHeaderPageID(prevHeaderPageID);
        //
        //         emptyPageHeaderPage.markAllUsed();
        //         prevPage.setNextHeaderPageNoID(headerPageID);
        //
        //         headerPageCount++;
        //         prevHeaderPageID = headerPageID;
        //
        //         // 刷盘
        //         writePageToDisk(prevPage);
        //         writePageToDisk(emptyPageHeaderPage);
        //     }
        // }
        //
        // // 获取emptyPageNo槽位真正所在的headerPage
        // BPTreeHeaderPage headerPage = (BPTreeHeaderPage) readPageFromDisk(headerPageID);
        // // 计算槽位在该页中的下标位置 ，设置状态为 unUsed
        // int slotIndex = pageNo - headerPageCount * BPTreeHeaderPage.maxSlotNum;
        // headerPage.markSlotUsed(slotIndex, false);
        //
        // // 刷盘
        // writePageToDisk(headerPage);
    }

    @Override
    public TableDesc getTableDesc() {
        return tableDesc;
    }

    @Override
    public BPTreePage readPageFromDisk(PageID pageID) {
        BPTreePageID bpTreePageID = (BPTreePageID) pageID;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            if (bpTreePageID.getPageType() == BPTreePageType.ROOT_PTR) {
                byte[] bytes = new byte[BPTreeRootPtrPage.rootPtrPageSizeInByte];
                int bytesRead = bis.read(bytes, 0, BPTreeRootPtrPage.rootPtrPageSizeInByte);
                if (bytesRead == -1) {
                    throw new IllegalArgumentException("BPTree file read from disk error：reach the end of the file");
                }
                if (bytesRead < BPTreeRootPtrPage.rootPtrPageSizeInByte) {
                    throw new IllegalArgumentException("未从 BPTree file 中读取到" + BPTreeRootPtrPage.rootPtrPageSizeInByte + "字节");
                }
                // System.out.println("BPTree file read  page ,pageNo=" + BPTreePageID.getPageNo() + ",BPTree type=" + BPTreePageID.getPageType());
                return new BPTreeRootPtrPage(bpTreePageID, bytes);
            } else {
                byte[] pageData = new byte[Page.defaultPageSizeInByte];
                long size = BPTreeRootPtrPage.rootPtrPageSizeInByte + (bpTreePageID.getPageNo() - 1) * Page.defaultPageSizeInByte;

                if (bis.skip(size) != size) {
                    throw new IllegalArgumentException("BPTree file read from disk error:寻址错误");
                }
                int bytesRead = bis.read(pageData, 0, Page.defaultPageSizeInByte);
                if (bytesRead == -1) {
                    throw new IllegalArgumentException("BPTree file read from disk error：reach the end of the file");
                }
                if (bytesRead < Page.defaultPageSizeInByte) {
                    throw new IllegalArgumentException("未从 BPTree file 中读取到" + Page.defaultPageSizeInByte + "字节");
                }

                // System.out.println("BPTree file read  page ,pageNo=" + BPTreePageID.getPageNo() + ",BPTree type=" + BPTreePageID.getPageType());

                if (bpTreePageID.getPageType() == BPTreePageType.HEADER) {
                    return new BPTreeHeaderPage(bpTreePageID, pageData);
                } else if (bpTreePageID.getPageType() == BPTreePageType.INTERNAL) {
                    return new BPTreeInternalPage(bpTreePageID, pageData, keyFieldIndex);
                } else { //BPTreePageID.getPageType() == BPTreePageType.LEAF
                    return new BPTreeLeafPage(bpTreePageID, pageData, keyFieldIndex);
                }
            }
        } catch (FileNotFoundException e) {
            throw new DbException("BPTree file not found", e);
        } catch (IOException e) {
            throw new DbException("BPTree file read page from disk error", e);
        }
    }

    @Override
    public void writePageToDisk(Page page) {
        try {
            byte[] pageData = page.serialize();
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            BPTreePageID pageID = (BPTreePageID) page.getPageID();
            if (pageID.getPageType() != BPTreePageType.ROOT_PTR) {
                rf.seek(BPTreeRootPtrPage.rootPtrPageSizeInByte + (page.getPageID().getPageNo() - 1) * Page.defaultPageSizeInByte);
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
        return (int) ((file.length() - BPTreeRootPtrPage.rootPtrPageSizeInByte) / Page.defaultPageSizeInByte);
    }

    /**
     * 获取rootPtr
     */
    private BPTreeRootPtrPage getRootPrtPage() throws IOException {
        // 首次在该页插入数据时，写入空数据
        synchronized (this) {
            if (file.length() == 0) {
                BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
                byte[] emptyRootPtrData = BPTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BPTreeLeafPage.createEmptyPageData();
                bos.write(emptyRootPtrData);
                bos.write(emptyLeafData);
                bos.close();
            }
        }
        // 获取单例的rootPage,如果不存在则新增
        BPTreePageID rootPtrPageID = new BPTreePageID(this.tableId, 0, BPTreePageType.ROOT_PTR);

        // 从磁盘中读取rootPage
        return (BPTreeRootPtrPage) DataBase.getBufferPool().getPage(rootPtrPageID);
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
    private BPTreeLeafPage splitLeafPage(BPTreeLeafPage leafPageNeedSplit, Field fieldToInsert) throws IOException {

        // 获取一个空页作为新的右兄弟
        BPTreeLeafPage newRightSibPage = (BPTreeLeafPage) getEmptyPage(BPTreePageType.LEAF);

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

        System.out.println("分列前的父页ID=" + leafPageNeedSplit.getParentPageID().getPageNo());
        BPTreeInternalPage parentInternalPage = findParentPageToPlaceEntry(leafPageNeedSplit.getParentPageID(), midKey);
        BPTreePageID oldRightSibPageID = leafPageNeedSplit.getRightSibPageID();

        // 更新新页的右兄弟、做兄弟，更新原页的右兄弟
        newRightSibPage.setRightSibPageID(oldRightSibPageID);
        newRightSibPage.setLeftSibPageID(leafPageNeedSplit.getPageID());
        leafPageNeedSplit.setRightSibPageID(newRightSibPage.getPageID());

        // 设置新页的父节点，新页和原页的父节点一定是同一个
        newRightSibPage.setParentPageID(parentInternalPage.getPageID());
        leafPageNeedSplit.setParentPageID(parentInternalPage.getPageID());
        if (newRightSibPage.getParentPageID().getPageNo() == 0) {
            throw new DbException("error");
        }


        // 父节点Page，插入midKey元素
        BPTreeEntry newParentEntry = new BPTreeEntry(midKey, leafPageNeedSplit.getPageID(), newRightSibPage.getPageID());
        parentInternalPage.insertEntry(newParentEntry);

        // 刷盘
        Connection.cacheDirtyPage(parentInternalPage);
        Connection.cacheDirtyPage(newRightSibPage);
        Connection.cacheDirtyPage(leafPageNeedSplit);

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
    private BPTreeInternalPage findParentPageToPlaceEntry(BPTreePageID parentPageID, Field keyToInsert) throws IOException {
        BPTreeInternalPage parentPage;
        // 如果原父节点是rootPtr，则新增一个internal page，并设置为新的rootNode
        if (parentPageID.getPageType() == BPTreePageType.ROOT_PTR) {
            // 设置新的rootNode
            parentPage = (BPTreeInternalPage) getEmptyPage(BPTreePageType.INTERNAL);
            BPTreeRootPtrPage rootPrtPage = getRootPrtPage();
            rootPrtPage.setRootNodePageID(parentPage.getPageID());
            Connection.cacheDirtyPage(rootPrtPage);
        } else {
            parentPage = (BPTreeInternalPage) DataBase.getBufferPool().getPage(parentPageID);
        }

        System.out.println("internal page getExistCount==" + parentPage.getExistCount() + ",pageNo=" + parentPage.getPageID().getPageNo());
        // 父节点没有空槽位则分裂
        if (!parentPage.hasEmptySlot()) {
            parentPage = splitInternalPage(parentPage, keyToInsert);

            // BPTreeRootPtrPage rootPrtPage = getRootPrtPage();
            // rootPrtPage.setRootNodePageID(parentPage.getPageID());
            // writePageToDisk(rootPrtPage);
        }
        Connection.cacheDirtyPage(parentPage);
        return parentPage;
    }

    /**
     * 分裂 internalPageNeedSplit 并返回可用的页
     *
     * @param internalPageNeedSplit 待分裂的页
     * @param keyToInsert           索引字段
     * @return 返回可用的页
     */
    @VisibleForTest
    public BPTreeInternalPage splitInternalPage(BPTreeInternalPage internalPageNeedSplit, Field keyToInsert) throws IOException {

        // 获取一个可用的页
        BPTreeInternalPage newInternalPage = (BPTreeInternalPage) getEmptyPage(BPTreePageType.INTERNAL);

        // 2:1 +1
        // 3：1+2
        // 4：2+2
        // 5：2+3
        //
        // int numToMove = internalPageNeedSplit.getExistCount() - (internalPageNeedSplit.getExistCount() / 2);
        int numToMove = internalPageNeedSplit.getExistCount() / 2;
        if (Math.abs(internalPageNeedSplit.getExistCount() / 2 - numToMove) >= 2) {
            throw new DbException("splitInternalPage 失败，元素没有均分");
        }

        Iterator<BPTreeEntry> it = internalPageNeedSplit.getReverseIterator();
        BPTreeEntry[] entryToMove = new BPTreeEntry[numToMove];
        int moveCnt = entryToMove.length - 1;
        BPTreeEntry midEntry = null;
        while (moveCnt >= 0 && it.hasNext()) {
            entryToMove[moveCnt--] = it.next();
        }

        // 将原页中右半部分的数据移入到新页中，右半部分的首个元素midEntry升级为上级索引
        for (int i = entryToMove.length - 1; i >= 0; --i) {
            if (i == 0) {
                // 从右侧开始删除
                internalPageNeedSplit.deleteEntryFromTheRight(entryToMove[i]);
                midEntry = entryToMove[0];
            } else {
                internalPageNeedSplit.deleteEntryFromTheRight(entryToMove[i]);
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
        BPTreeInternalPage newParentInternalPage = findParentPageToPlaceEntry(internalPageNeedSplit.getParentPageID(), midEntry.getKey());
        newParentInternalPage.insertEntry(midEntry);
        internalPageNeedSplit.setParentPageID(newParentInternalPage.getPageID());
        newInternalPage.setParentPageID(newParentInternalPage.getPageID());

        // newInternalPage、newInternalPage、newParentInternalPage 刷盘
        Connection.cacheDirtyPage(internalPageNeedSplit);
        Connection.cacheDirtyPage(newInternalPage);
        Connection.cacheDirtyPage(newParentInternalPage);

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
    private void updateParent(BPTreePageID newParentPageID, BPTreePageID childPageID) throws IOException {
        BPTreePage childPage = (BPTreePage) DataBase.getBufferPool().getPage(childPageID);
        if (!childPage.getParentPageID().equals(newParentPageID)) {
            childPage = (BPTreePage) DataBase.getBufferPool().getPage(childPageID);
            childPage.setParentPageID(newParentPageID);

            // TODO 缓存+集中刷盘
            Connection.cacheDirtyPage(childPage);
        }
    }

    /**
     * 返回一个空叶页
     */
    private Page getEmptyPage(int pageType) throws IOException {
        int getExistPageCountExceptRootPtr = getEmptyPageNo();
        int newPageNo = getExistPageCountExceptRootPtr;// pageNo 从0开始计数
        this.writeEmptyPageToDisk(newPageNo, pageType);
        BPTreePageID bpTreePageID = new BPTreePageID(tableId, newPageNo, pageType);
        return DataBase.getBufferPool().getPage(bpTreePageID);
    }

    /**
     * 写入文件一个新页
     *
     * @param pageNo   写入的页号
     * @param pageType 页类型
     */
    private void writeEmptyPageToDisk(int pageNo, int pageType) throws IOException {
        // TODO opt
        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        if (pageType == BPTreePageType.ROOT_PTR) {
            rf.seek(0);
            rf.write(new byte[BPTreeRootPtrPage.rootPtrPageSizeInByte]);
        } else {
            int pageSizeInByte = Page.defaultPageSizeInByte; // 除 ROOT_PTR之外的页大小
            rf.seek(BPTreeRootPtrPage.rootPtrPageSizeInByte + (pageNo - 1) * pageSizeInByte);
            rf.write(new byte[pageSizeInByte]);
        }
        rf.close();
    }

    /**
     * 返回一个空的pageNo
     * 获取本文件的根page，找到第一个header page id
     */
    private int getEmptyPageNo() throws IOException {
        BPTreeRootPtrPage rootPrtPage = getRootPrtPage();
        BPTreePageID headerPageID = rootPrtPage.getFirstHeaderPageID();

        int emptyPageNo = 0;

        // TODO 暂不实现
        // if (headerPageID != null) {
        //     // 读取第一个headerPage
        //     BPTreeHeaderPage headerPage = (BPTreeHeaderPage) this.readPageFromDisk(headerPageID);
        //     int headerPageCount = 0;
        //     // 遍历headerPage，找到一个含有空槽位的page
        //     while (headerPage != null && !headerPage.hasEmptySlot()) { // header页存在，但没有空槽位
        //         headerPageID = headerPage.getNextPageID();
        //         if (headerPageID != null) {
        //             headerPage = (BPTreeHeaderPage) this.readPageFromDisk(headerPageID);
        //             headerPageCount++;
        //         } else {
        //             break; //遍历完现有的headerPage，全都没有空槽位。
        //         }
        //     }
        //
        //     // 如果headerPage != null，一定存在空的slot
        //     if (headerPage != null) {
        //         headerPage = (BPTreeHeaderPage) this.readPageFromDisk(headerPageID);
        //         int emptySlot = headerPage.getFirstEmptySlot();
        //         headerPage.markSlotUsed(emptySlot, true);
        //         emptyPageNo = headerPageCount * BPTreeHeaderPage.maxSlotNum + emptySlot;
        //     }
        // }

        // 没有headerPage或现有的HeaderPage均没有空槽位，直接新增一个新页
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

    private BPTreeLeafPage findLeafPageWithIndex(BPTreePageID rootNodePageID,
                                                 IndexPredicate indexPredicate) {
        return null;
    }

    /**
     * 查找索引field应该放置的页面，不考虑是否已满
     * 根据pageID和索引查找叶子页
     *
     * @param pageID     数据的根节点PageID
     * @param indexField 索引字段值，在内部节点的查找过程中使用,null 时返回最左page
     * @return 查找field应该放置的页面
     */
    private BPTreeLeafPage findLeafPage(BPTreePageID pageID, Field indexField) {

        // 查找到树的最后一层--leafPage
        if (pageID.getPageType() == BPTreePageType.LEAF) {
            return (BPTreeLeafPage) DataBase.getBufferPool().getPage(pageID);
        }

        // 查找到树的内部节点，在这里递归直至查找到leafPage
        BPTreeInternalPage aInternalPage = (BPTreeInternalPage) DataBase.getBufferPool().getPage(pageID);
        BPTreeEntry entry;

        Iterator<BPTreeEntry> iterator = aInternalPage.getIterator();
        entry = iterator.next();

        BPTreePageID nextSearchPageId;
        if (indexField == null) { // 特殊情况，对于null值，一律从左树中查找
            nextSearchPageId = entry.getLeftChildPageID();
        } else {
            // 如果查找的值>节点值，判断树中下一个节点
            while (indexField.compare(PredicateEnum.GREATER_THAN, entry.getKey()) && iterator.hasNext()) {
                entry = iterator.next();
            }

            // 如果查找的值<=节点值，进入树的左子树
            if (indexField.compare(PredicateEnum.LESS_THAN_OR_EQ, entry.getKey())) {
                nextSearchPageId = entry.getLeftChildPageID();
            } else {
                // 如果查找的值>节点值，进入树的右子树
                nextSearchPageId = entry.getRightChildPageID();
            }
        }

        System.out.println("B+Tree中查询页面，nextSearchPageId=" + nextSearchPageId);
        return this.findLeafPage(nextSearchPageId, indexField);
    }

    @Override
    public ITableFileIterator getIterator() {
        return new BPTreeTableFileIterator(this);
    }

    public ITableFileIterator getIndexIterator(IndexPredicate indexPredicate) {
        return new BPTreeIndexIterator(this, indexPredicate);
    }

    //====================================迭代器======================================
    private static class BPTreeTableFileIterator implements ITableFileIterator {

        /**
         * 当前页
         */
        private BPTreeLeafPage curPage;
        /**
         * 行迭代器
         */
        private Iterator<Row> rowIterator;

        /**
         * 文件
         */
        private BPTreeTableFile bpTreeTableFile;

        private Row next = null;

        public BPTreeTableFileIterator(BPTreeTableFile bpTreeTableFile) {
            this.bpTreeTableFile = bpTreeTableFile;
        }

        @Override
        public void open() throws DbException {
            // 找到文件的根指针页，拿到根节点页ID，从根节点找到第一个叶子页面
            BPTreeRootPtrPage page = (BPTreeRootPtrPage) DataBase.getBufferPool()
                    .getPage(BPTreeRootPtrPage.getRootPtrPageID(bpTreeTableFile.getTableId()));
            this.curPage = bpTreeTableFile.findLeafPage(page.getRootNodePageID(), null);
            this.rowIterator = curPage.getRowIterator();
        }

        @Override
        public boolean hasNext() throws DbException {
            if (next == null) {
                next = readNext();
            }
            return next != null;
        }

        @Override
        public Row next() throws DbException, NoSuchElementException {
            if (next == null) {
                next = readNext();
                if (next == null) {
                    throw new NoSuchElementException();
                }
            }
            Row result = next;
            next = null;
            return result;
        }

        @Override
        public void close() {
            rowIterator = null;
            curPage = null;
        }

        private Row readNext() {
            // 本页读取完
            if (rowIterator != null && !rowIterator.hasNext()) {
                rowIterator = null;
            }

            // 本页读取完后，读取下一页,在叶子节点层读取，读取右兄弟获取下一页pageID
            while (rowIterator == null && curPage != null) {
                BPTreePageID nextPageID = curPage.getRightSibPageID();
                if (nextPageID == null) {
                    curPage = null;
                } else {
                    curPage = (BPTreeLeafPage) DataBase.getBufferPool().getPage(nextPageID);
                    rowIterator = curPage.getRowIterator();
                    if (!rowIterator.hasNext()) {
                        rowIterator = null;
                    }
                }
            }

            // 文件中没有数据
            if (rowIterator == null) {
                return null;
            }
            return rowIterator.next();
        }
    }

    private static class BPTreeIndexIterator implements ITableFileIterator {
        private BPTreeTableFile bpTreeTableFile;
        private IndexPredicate indexPredicate;
        private BPTreeLeafPage curPage;
        private Iterator<Row> rowIterator;
        private Row next = null;

        public BPTreeIndexIterator(BPTreeTableFile bpTreeTableFile, IndexPredicate indexPredicate) {
            this.bpTreeTableFile = bpTreeTableFile;

            if (!PredicateEnum.BPTREE_INDEX_PREDICATES.contains(indexPredicate.getPredicate())) {
                throw new DbException("B+Tree索引搜索暂不支持" + indexPredicate.getPredicate());
            }
            this.indexPredicate = indexPredicate;
        }

        @Override
        public void open() throws DbException {
            BPTreeRootPtrPage page =
                    (BPTreeRootPtrPage) DataBase.getBufferPool().getPage(BPTreeRootPtrPage.getRootPtrPageID(bpTreeTableFile.getTableId()));
            BPTreePageID rootNodePageID = page.getRootNodePageID();
            if (PredicateEnum.BPTREE_INDEX_PREDICATES.contains(indexPredicate.getPredicate())) {
                curPage = bpTreeTableFile.findLeafPage(rootNodePageID, indexPredicate.getParamOperand());
            } else {
                curPage = bpTreeTableFile.findLeafPage(rootNodePageID, null);
            }
            rowIterator = curPage.getRowIterator();
        }

        @Override
        public boolean hasNext() throws DbException {
            if (next == null) next = readNext();
            return next != null;
        }

        private Row readNext() {
            while (rowIterator != null) {
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();

                    // 判断是否满足条件
                    if (row.getField(bpTreeTableFile.getKeyFieldIndex()).compare(indexPredicate.getPredicate(),
                            indexPredicate.getParamOperand())) {
                        return row;
                    } else if (indexPredicate.getPredicate() == PredicateEnum.EQUALS
                            // 所有元素均大于给定的参数
                            && row.getField(bpTreeTableFile.getKeyFieldIndex()).compare(PredicateEnum.GREATER_THAN,
                            indexPredicate.getParamOperand())) {
                        return null;
                    } else if (indexPredicate.getPredicate() == PredicateEnum.LESS_THAN
                            || indexPredicate.getPredicate() == PredicateEnum.LESS_THAN_OR_EQ) {
                        // 不支持'小于'查询
                        return null;
                    }
                }

                BPTreePageID nextPage = curPage.getRightSibPageID();
                if (nextPage == null) {
                    return null;
                } else {
                    curPage = (BPTreeLeafPage) DataBase.getBufferPool().getPage(nextPage);
                    rowIterator = curPage.getRowIterator();
                }
            }

            return null;
        }

        @Override
        public Row next() throws DbException, NoSuchElementException {
            if (next == null) {
                next = readNext();
                if (next == null) {
                    throw new NoSuchElementException();
                }
            }
            Row result = next;
            next = null;
            return result;
        }

        @Override
        public void close() {
            rowIterator = null;
            curPage = null;
        }
    }

}
