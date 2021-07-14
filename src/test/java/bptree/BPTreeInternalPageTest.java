package bptree;

import com.microdb.model.DataBase;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.BPTreeTableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.bptree.*;
import com.microdb.operator.Delete;
import com.microdb.operator.bptree.BPTreeScan;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * BPTreeInternalPageTest
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeInternalPageTest {
    public DataBase dataBase;

    private TableDesc personTableDesc;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        BPTreeTableFile bpTreeTableFile = new BPTreeTableFile(dataBase, file, tableDesc, 0);
        dataBase.addTable(bpTreeTableFile, "t_person");
        this.dataBase = dataBase;

        personTableDesc = tableDesc;
    }

    public static final int[][] EXAMPLE_VALUES = new int[][]{
            {2, 6350, 4},
            {4, 9086, 5},
            {5, 17197, 7},
            {7, 22064, 9},
            {9, 22189, 10},
            {10, 28617, 11},
            {11, 31933, 13},
            {13, 33549, 14},
            {14, 34784, 15},
            {15, 42878, 17},
            {17, 45569, 19},
            {19, 56462, 20},
            {20, 62778, 21},
            {15, 42812, 16},
            {2, 3596, 3},
            {6, 17876, 7},
            {1, 1468, 2},
            {11, 29402, 12},
            {18, 51440, 19},
            {7, 19209, 8}
    };

    /**
     * 添加entry
     */
    @Test
    public void insertEntry() throws Exception {
        byte[] data = BPTreeInternalPage.createEmptyPageData(dataBase.getDBConfig().getPageSizeInByte());
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(dataBase, pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        int[][] EXAMPLE_VALUES = new int[][]{
                {2, 100, 4},
                {4, 200, 5},
                {5, 300, 7}
        };


        // 超量插入：报错
        EXAMPLE_VALUES = new int[][]{
                {2, 6350, 4},
                {4, 9086, 5},
                {5, 17197, 7},
                {7, 22064, 9},
                {9, 22189, 10},
                {10, 28617, 11},
        };
        for (int[] entry : EXAMPLE_VALUES) {
            BPTreePageID leftChild = new BPTreePageID(pageID.getTableId(), entry[0], BPTreePageType.LEAF);
            BPTreePageID rightChild = new BPTreePageID(pageID.getTableId(), entry[2], BPTreePageType.LEAF);
            BPTreeEntry e = new BPTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            entries.add(e);
            page.insertEntry(e);
        }

        System.out.println(page);

    }

    @Test
    public void testIterator() throws IOException {
        byte[] data = BPTreeInternalPage.createEmptyPageData(dataBase.getDBConfig().getPageSizeInByte());
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(dataBase, pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        int[][] EXAMPLE_VALUES = new int[][]{
                {2, 100, 4},
                {4, 200, 5},
                {5, 300, 7}
        };

        // 从小到大顺序插入
        for (int[] entry : EXAMPLE_VALUES) {
            BPTreePageID leftChild = new BPTreePageID(pageID.getTableId(), entry[0], BPTreePageType.LEAF);
            BPTreePageID rightChild = new BPTreePageID(pageID.getTableId(), entry[2], BPTreePageType.LEAF);
            BPTreeEntry e = new BPTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            entries.add(e);
            page.insertEntry(e);
        }


        System.out.println("正向迭代器---");
        Iterator<BPTreeEntry> iterator = page.getIterator();
        while (iterator.hasNext()) {
            BPTreeEntry next = iterator.next();
            System.out.println(next);
        }

        System.out.println("===========");
        iterator = page.getIterator();
        System.out.println(iterator.next());
        System.out.println(iterator.next());
        System.out.println(iterator.next());

        try {
            iterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException e) {
        }

        System.out.println("反向迭代器---");
        Iterator<BPTreeEntry> reverseIterator = page.getReverseIterator();
        while (reverseIterator.hasNext()) {
            BPTreeEntry next = reverseIterator.next();
            System.out.println(next);
        }

        System.out.println("====================");

        reverseIterator = page.getReverseIterator();
        System.out.println(reverseIterator.next());
        System.out.println(reverseIterator.next());
        System.out.println(reverseIterator.next());

        try {
            iterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException e) {
        }

    }


    /**
     * delete Entry
     */
    @Test
    public void deleteEntry() throws Exception {

        byte[] data = BPTreeInternalPage.createEmptyPageData(dataBase.getDBConfig().getPageSizeInByte());
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(dataBase, pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        int[][] EXAMPLE_VALUES = new int[][]{
                {2, 100, 4},
                {4, 200, 5},
                {5, 300, 7}
        };

        // 从小到大顺序插入
        for (int[] entry : EXAMPLE_VALUES) {
            BPTreePageID leftChild = new BPTreePageID(pageID.getTableId(), entry[0], BPTreePageType.LEAF);
            BPTreePageID rightChild = new BPTreePageID(pageID.getTableId(), entry[2], BPTreePageType.LEAF);
            BPTreeEntry e = new BPTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            entries.add(e);
            page.insertEntry(e);
        }

        Iterator<BPTreeEntry> iterator = page.getIterator();
        iterator = page.getIterator();
        BPTreeEntry next1 = iterator.next();
        BPTreeEntry next2 = iterator.next();
        BPTreeEntry next3 = iterator.next();

        // page.deleteEntryFromTheLeft(next1);
        // page.deleteEntryFromTheLeft(next2);
        // page.deleteEntryFromTheLeft(next3);
        // printEntry(page);

        // page.deleteEntryFromTheRight(next1);
        page.deleteEntryFromTheRight(next2);
        // page.deleteEntryFromTheRight(next3);
        printInternalPage(page);
    }


    @Test
    public void insertEntryAndSplit() throws IOException {
        byte[] data = BPTreeInternalPage.createEmptyPageData(dataBase.getDBConfig().getPageSizeInByte());
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(dataBase, pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        System.out.println(page.getPageID());
        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();

        for (int i = 1; i < 11; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            tableFile.insertRow(row);
        }
        int tableId = tableFile.getTableId();
        BPTreePageID rootPtrPageID = BPTreeRootPtrPage.getRootPtrPageID(tableId);
        BPTreeRootPtrPage rootPtrPage;
        BPTreePage rootNodePage;


        Row row = new Row(personTableDesc);
        row.setField(0, new IntField(11));
        row.setField(1, new IntField(18));
        tableFile.insertRow(row);

        rootPtrPage = (BPTreeRootPtrPage) dataBase.getBufferPool().getPage(rootPtrPageID);
        rootNodePage = (BPTreePage) dataBase.getBufferPool().getPage(rootPtrPage.getRootNodePageID());
        System.out.println("打印rootNodePage======");
        IntField key = (IntField) ((BPTreeInternalPage) rootNodePage).getIterator().next().getKey();
        assertTrue(key.getValue() == 5);
        Iterator<BPTreeEntry> iterator = ((BPTreeInternalPage) rootNodePage).getIterator();

        while (iterator.hasNext()) {
            BPTreeEntry next = iterator.next();
            // should be internal

            System.out.println("left=====:");
            BPTreePage bpTreePage = (BPTreePage) dataBase.getBufferPool().getPage(next.getLeftChildPageID());
            printInternalPage((BPTreeInternalPage) bpTreePage);
            printLeafByInternalPage(tableFile, (BPTreeInternalPage) bpTreePage);


            System.out.println("right=====:");
            BPTreePage bpTreePage1 = (BPTreePage) dataBase.getBufferPool().getPage(next.getRightChildPageID());
            printInternalPage((BPTreeInternalPage) bpTreePage1);
            printLeafByInternalPage(tableFile, (BPTreeInternalPage) bpTreePage1);
        }
    }


    /**
     * 删除测试
     * 表中只有一页internalPage的情况
     */
    @Test
    public void deleteRowAndEntryOneInternalPage() throws IOException {
        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        for (int i = 1; i <= 10; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            tableFile.insertRow(row);
        }

        BPTreeScan scan = new BPTreeScan(tableFile.getTableId(), null);

        System.out.println("开始打印====");
        // 删除并打印
        for (int i = 1; i <= 10; i++) {
            deleteOne(tableFile, scan);
            printTree(tableFile, tableId);
        }


        scan.open();
        assertFalse(scan.hasNext());
    }

    /**
     * 删除测试
     * 表中超过1页internalPage，但internal page 均不满的情况
     */
    @Test
    public void deleteRowAndEntryForSeveralInternalPage() throws IOException {
        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        int num = 200;
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            dataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");
        printTree(tableFile, tableId);

        BPTreeScan scan = new BPTreeScan(tableFile.getTableId(), null);

        System.out.println("开始打印====");
        // 删除并打印
        for (int i = 1; i <= num; i++) {
            deleteOne(tableFile, scan);
            printTree(tableFile, tableId);
        }

        scan.open();
        assertFalse(scan.hasNext());
    }


    /**
     * 删除测试
     * 大量数据
     */
    @Test
    public void deleteRowAndEntryMassData() throws IOException {
        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        int num = 30000;
        long l1 = System.currentTimeMillis();
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            dataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");
        printTree(tableFile, tableId);

        BPTreeScan scan = new BPTreeScan(tableFile.getTableId(), null);

        scan.open();
        while (scan.hasNext()) {
            System.out.println(scan.next());

        }

        // 通过Delete操作符删除，执行非常快速
        Delete delete = new Delete(scan);
        delete.loopDelete();

        // 外部迭代删除，删除非常慢
        // System.out.println("开始打印====");
        // // 删除并打印
        // for (int i = 1; i <= num; i++) {
        //     deleteOne(tableFile, scan);
        //     printTree(tableFile, tableId);
        // }


        scan.open();
        assertFalse(scan.hasNext());
        System.out.println("耗时ms:" + (System.currentTimeMillis() - l1));
    }


    private void deleteOne(BPTreeTableFile tableFile, BPTreeScan scan) throws IOException {
        scan.open();
        Row next = scan.next();
        // tableFile.deleteRow(scan.next());
        dataBase.getBufferPool().deleteRow(next);
        scan.close();
    }

    private void printTree(BPTreeTableFile tableFile, int tableId) {
        BPTreePageID rootPtrPageID = BPTreeRootPtrPage.getRootPtrPageID(tableId);
        BPTreeRootPtrPage rootPtrPage = (BPTreeRootPtrPage) dataBase.getBufferPool().getPage(rootPtrPageID);
        BPTreePage rootNodePage = (BPTreePage) dataBase.getBufferPool().getPage(rootPtrPage.getRootNodePageID());
        System.out.println("打印rootNodePage======");
        if (rootNodePage instanceof BPTreeInternalPage) {
            printBPTreePage(rootNodePage);
            Iterator<BPTreeEntry> iterator = ((BPTreeInternalPage) rootNodePage).getIterator();
            System.out.println("打印内部元素=====");
            while (iterator.hasNext()) {
                BPTreeEntry next = iterator.next();

                System.out.println("left=====:");
                BPTreePage bpTreePage = (BPTreePage) dataBase.getBufferPool().getPage(next.getLeftChildPageID());
                printBPTreePage(bpTreePage);

                System.out.println("right=====:");
                BPTreePage bpTreePage1 = (BPTreePage) dataBase.getBufferPool().getPage(next.getRightChildPageID());
                printBPTreePage(bpTreePage1);
            }
        } else if (rootNodePage instanceof BPTreeLeafPage) {
            printBPTreePage(rootNodePage);
        }

    }

    private void printBPTreePage(BPTreePage bpTreePage) {
        if (bpTreePage instanceof BPTreeInternalPage) {
            printInternalPage((BPTreeInternalPage) bpTreePage);
        } else if (bpTreePage instanceof BPTreeLeafPage) {
            printLeafPage(bpTreePage);
        }
    }

    private void printLeafByInternalPage(BPTreeTableFile tableFile, BPTreeInternalPage bpTreePage) {
        Iterator<BPTreeEntry> iterator1 = bpTreePage.getIterator();
        while (iterator1.hasNext()) {
            BPTreeEntry next1 = iterator1.next();
            printLeafPage((BPTreePage) dataBase.getBufferPool().getPage(next1.getLeftChildPageID()));
            printLeafPage((BPTreePage) dataBase.getBufferPool().getPage(next1.getRightChildPageID()));
        }
    }

    private void printLeafPage(BPTreePage bpTreePage) {
        Iterator<Row> rowIterator = bpTreePage.getRowIterator();
        while (rowIterator.hasNext()) {
            Row next1 = rowIterator.next();
            System.out.println(next1);
        }
    }

    public void checkChildCorrect() {

    }

    public void printInternalPage(BPTreeInternalPage page) {
        Iterator<BPTreeEntry> iterator = page.getIterator();
        while (iterator.hasNext()) {
            BPTreeEntry next = iterator.next();
            System.out.println(next);
        }
    }

}
